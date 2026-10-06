package ch.epfl.coshim.net;

import ch.epfl.coshim.core.CoShim;
import ch.epfl.coshim.core.NoCcShim;
import ch.epfl.coshim.core.SpeculativeCoShim;
import ch.epfl.coshim.store.InMemoryKvStore;
import ch.epfl.coshim.store.KvStore;
import ch.epfl.coshim.store.TableKey;
import ch.epfl.coshim.store.sql.SqlKvStore;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Runs one shim node hosting one or more databases, one shim each (String keys and values), each
 * on its own store: in memory, or a MySQL / PostgreSQL database ({@link SqlKvStore}).
 *
 * <pre>
 *   COSHIM_TOKEN=secret java -cp ... ch.epfl.coshim.net.CoShimNode [--port P] [--shim S] [--databases D]
 *       [--lock-timeout-ms T] [--stats-interval-s I] [--allow HOST ...]
 *       [--store memory|JDBC-URL] [--store-pool-size N]
 *     --port              default 7000
 *     --shim              speculative (pseudo.txt) | nonspeculative (strict 2PL baseline: lock() waits
 *                         for the predecessors to commit, no cascades) | nocc (no concurrency
 *                         control); default nocc
 *     --databases         comma-separated database names; default "default"
 *     --lock-timeout-ms   lock_timeout of speculative/nonspeculative; default 200
 *     --stats-interval-s  period of the commit/abort counters log line, 0 = only at shutdown; default 10
 *     --allow             a client address allowed to connect (e.g. an RM host), repeatable; default:
 *                         any authenticated client
 *     --store             where each database's data lives: memory (default), or a jdbc:mysql: /
 *                         jdbc:postgresql: URL. With several databases the URL must contain
 *                         {database}, replaced by each database's name (one SQL database each)
 *     --store-pool-size   connections per SQL store; default 32
 *
 *   Positional form (still accepted): CoShimNode [port] [shim] [databases] [allowed-client ...]
 * </pre>
 *
 * RMs then use one data source per database:
 * {@code new CoShimDataSource<>("host:7000/orders", new RemoteCoShim<>(node, "orders", ...))}.
 */
public final class CoShimNode {

    private CoShimNode() {}

    /** The parsed command line. */
    record Options(int port, String shim, String[] databases, Duration lockTimeout, long statsIntervalSeconds,
            Set<InetAddress> allowed, String store, int storePoolSize) {

        static final String DATABASE_PLACEHOLDER = "{database}";

        static Options parse(String[] args) throws Exception {
            int port = 7000;
            String shim = "nocc";
            // A comma-separated list, e.g. "shop,billing": the node hosts one database (and one shim) per
            // name. With no argument it hosts a single database called "default".
            String databases = "default";
            long lockTimeoutMs = 200;
            long statsIntervalSeconds = 10;
            Set<InetAddress> allowed = new HashSet<>();
            String store = "memory";
            int storePoolSize = SqlKvStore.DEFAULT_POOL_SIZE;
            if (args.length > 0 && args[0].startsWith("--")) {
                for (int i = 0; i < args.length; i++) {
                    String flag = args[i];
                    if (i + 1 >= args.length) {
                        throw new IllegalArgumentException(flag + " needs a value");
                    }
                    String value = args[++i];
                    switch (flag) {
                        case "--port" -> port = Integer.parseInt(value);
                        case "--shim" -> shim = value;
                        case "--databases" -> databases = value;
                        case "--lock-timeout-ms" -> lockTimeoutMs = Long.parseLong(value);
                        case "--stats-interval-s" -> statsIntervalSeconds = Long.parseLong(value);
                        case "--allow" -> allowed.add(InetAddress.getByName(value));
                        case "--store" -> store = value;
                        case "--store-pool-size" -> storePoolSize = Integer.parseInt(value);
                        default -> throw new IllegalArgumentException("unknown option " + flag);
                    }
                }
            } else {
                port = args.length > 0 ? Integer.parseInt(args[0]) : port;
                shim = args.length > 1 ? args[1] : shim;
                databases = args.length > 2 ? args[2] : databases;
                for (int i = 3; i < args.length; i++) {
                    allowed.add(InetAddress.getByName(args[i]));
                }
            }
            String[] names = databases.split(",");
            if (!store.equals("memory")) {
                SqlKvStore.Dialect.of(store);   // fail on a bad URL before anything starts
                if (names.length > 1 && !store.contains(DATABASE_PLACEHOLDER)) {
                    throw new IllegalArgumentException("--store must contain " + DATABASE_PLACEHOLDER
                            + " when the node hosts several databases (one SQL database each): " + store);
                }
            }
            return new Options(port, shim, names, Duration.ofMillis(lockTimeoutMs), statsIntervalSeconds, allowed,
                    store, storePoolSize);
        }

        /** The JDBC URL of {@code database}'s store, or null for an in-memory store. */
        String storeUrl(String database) {
            return store.equals("memory") ? null : store.replace(DATABASE_PLACEHOLDER, database);
        }

        KvStore<TableKey<String>, String> newStore(String database) {
            String url = storeUrl(database);
            return url == null ? new InMemoryKvStore<>() : SqlKvStore.open(url, storePoolSize);
        }

        CoShim<TableKey<String>, String> newShim(KvStore<TableKey<String>, String> store) {
            return switch (shim) {
                case "speculative" -> new SpeculativeCoShim<>(store, lockTimeout,
                        SpeculativeCoShim.DEFAULT_TOMBSTONE_TTL, true);
                case "nonspeculative" -> new SpeculativeCoShim<>(store, lockTimeout,
                        SpeculativeCoShim.DEFAULT_TOMBSTONE_TTL, false);
                case "nocc" -> new NoCcShim<>(store);
                default -> throw new IllegalArgumentException("unknown shim " + shim);
            };
        }
    }

    public static void main(String[] args) throws Exception {
        String token = System.getenv("COSHIM_TOKEN");
        if (token == null || token.isEmpty()) {
            System.err.println("set COSHIM_TOKEN: the shared secret the RMs must present");
            System.exit(2);
        }
        Options options = Options.parse(args);

        Map<String, CoShim<TableKey<String>, String>> databases = new LinkedHashMap<>();
        for (String name : options.databases()) {
            KvStore<TableKey<String>, String> store = options.newStore(name);
            if (store instanceof SqlKvStore sql) {
                Runtime.getRuntime().addShutdownHook(new Thread(sql::close, "coshim-store-close-" + name));
            }
            databases.put(name, options.newShim(store));
        }
        CoShimServer<TableKey<String>, String> server = new CoShimServer<>(
                databases, new TableKeyCodec<>(Codec.UTF8), Codec.UTF8, new InetSocketAddress(options.port()), token,
                options.allowed());
        System.out.println("coshim node (" + options.shim() + ", lock timeout " + options.lockTimeout().toMillis()
                + " ms) on port " + server.getPort() + ", databases " + databases.keySet()
                + ", store " + options.store()
                + (options.allowed().isEmpty() ? "" : ", allowed clients " + options.allowed()));

        Runnable printStats = () -> databases.forEach((name, shim) -> {
            if (shim instanceof SpeculativeCoShim<?, ?> speculative) {
                System.out.println("stats " + name + ": " + speculative.stats());
            }
        });
        Runtime.getRuntime().addShutdownHook(new Thread(printStats, "coshim-stats-final"));
        if (options.statsIntervalSeconds() > 0) {
            ScheduledExecutorService statsLogger = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "coshim-stats");
                t.setDaemon(true);
                return t;
            });
            statsLogger.scheduleAtFixedRate(printStats, options.statsIntervalSeconds(),
                    options.statsIntervalSeconds(), TimeUnit.SECONDS);
        }
        Thread.currentThread().join();
    }
}
