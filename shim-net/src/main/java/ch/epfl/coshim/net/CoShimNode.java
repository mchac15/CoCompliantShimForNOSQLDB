package ch.epfl.coshim.net;

import ch.epfl.coshim.core.CoShim;
import ch.epfl.coshim.core.NoCcShim;
import ch.epfl.coshim.core.SpeculationAdmission;
import ch.epfl.coshim.core.SpeculativeCoShim;
import ch.epfl.coshim.store.KvStore;
import ch.epfl.coshim.store.TableKey;
import ch.epfl.coshim.store.sql.KvStores;
import ch.epfl.coshim.types.DeadlockPolicy;
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
 * on its own store: in memory, or a MySQL / PostgreSQL database ({@link KvStores}).
 *
 * <pre>
 *   COSHIM_TOKEN=secret java -cp ... ch.epfl.coshim.net.CoShimNode [--port P] [--shim S] [--databases D]
 *       [--lock-timeout-ms T] [--deadlock P] [--prepare-deadlock Q] [--stats-interval-s I] [--allow HOST ...]
 *       [--store memory|URL] [--adm-r-max X|auto] [--adm-tau-ms T] [--adm-p-min P] [--adm-p-max P]
 *       [--adm-width W] [--adm-reach-cap C] [--adm-prior-mean M] [--adm-prior-weight W]
 *       [--adm-mode threshold|ratio] [--adm-eta E]
 *     --port              default 7000
 *     --shim              speculative (pseudo.txt) | nonspeculative (strict 2PL baseline: lock() waits
 *                         for the predecessors to commit, no cascades) | admission (speculative, but
 *                         each txn speculates or not as SpeculationAdmission decides at its start)
 *                         | nocc (no concurrency control); default nocc
 *     --adm-mode          admission: threshold (default: p from r against r*, the options below) |
 *                         ratio (both modes side by side, p += eta ln(E_spec / E_nonspec) every 250 ms,
 *                         E = commits per second of branch lifetime; p within --adm-p-min..--adm-p-max)
 *     --adm-eta           admission ratio: the step size eta; default 0.1
 *     --adm-r-max         admission: the threshold r* on r, the share of speculating txns that cascades
 *                         abort; auto (default): the measured break-even saved / (saved + lost)
 *     --adm-tau-ms        admission: decay time constant of r, saved and lost; default 1000
 *     --adm-p-min         admission: share of txns that speculate once r >= r* + width; default 0.05
 *     --adm-p-max         admission: share of txns that speculate while r <= r* - width; default 1
 *                         (ratio mode: 0.95, so that both groups are always measured)
 *     --adm-width         admission: half-width of the linear ramp between the two around r*; 0: a
 *                         hard switch; default 0.1
 *     --adm-reach-cap     admission: cap on an outcome's reach (saved x (1 + dependents), lost x (1 +
 *                         fan-out)); 0: no weighting; default 10
 *     --adm-prior-mean    admission: the share assumed without evidence; default 0.05
 *     --adm-prior-weight  admission: how many outcomes that assumption is worth; default 3
 *     --databases         comma-separated database names; default "default"
 *     --lock-timeout-ms   lock_timeout of speculative/nonspeculative; default 200
 *     --deadlock          deadlock prevention of speculative/nonspeculative: wait-die (an older txn
 *                         dies instead of waiting for a younger one) | wound-wait (it aborts the
 *                         younger running one instead) | none (just wait until lock_timeout: only for
 *                         workloads that cannot deadlock, e.g. ordered locking); default wait-die
 *     --prepare-deadlock  cross-shim deadlock prevention in prepare(): none | wait-die (prepare() votes
 *                         NO instead of waiting for a younger txn; the ages then come from the Seata
 *                         global transaction id, the same on every shim); default none
 *     --stats-interval-s  period of the commit/abort counters log line, 0 = only at shutdown; default 10
 *     --allow             a client address allowed to connect (e.g. an RM host), repeatable; default:
 *                         any authenticated client
 *     --store             memory (default), or a server URL jdbc:coshim:mysql://host:port or
 *                         jdbc:coshim:pg://host:port, with SqlKvStore's parameters (?tables=...&
 *                         entries=N&pool=N): each database D of the node is stored in the SQL
 *                         database D on that server (which must exist)
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
    record Options(int port, String shim, String[] databases, Duration lockTimeout, DeadlockPolicy deadlock,
            boolean prepareWaitDie, long statsIntervalSeconds, Set<InetAddress> allowed, String store,
            SpeculationAdmission.Config admission) {

        static Options parse(String[] args) throws Exception {
            int port = 7000;
            String shim = "nocc";
            // A comma-separated list, e.g. "shop,billing": the node hosts one database (and one shim) per
            // name. With no argument it hosts a single database called "default".
            String databases = "default";
            long lockTimeoutMs = 200;
            DeadlockPolicy deadlock = DeadlockPolicy.WAIT_DIE;
            boolean prepareWaitDie = false;
            long statsIntervalSeconds = 10;
            Set<InetAddress> allowed = new HashSet<>();
            String store = "memory";
            SpeculationAdmission.Config adm = SpeculationAdmission.Config.DEFAULT;
            double admRMax = adm.rMax();
            long admTauMs = adm.tau().toMillis();
            double admPMin = adm.pMin();
            Double admPMax = null;   // default: the mode's
            SpeculationAdmission.Mode admMode = adm.mode();
            double admEta = adm.eta();
            double admWidth = adm.width();
            int admReachCap = adm.reachCap();
            double admPriorMean = adm.priorMean();
            double admPriorWeight = adm.priorWeight();
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
                        case "--deadlock" -> deadlock = parseDeadlock(value);
                        case "--prepare-deadlock" -> prepareWaitDie = parsePrepareDeadlock(value);
                        case "--stats-interval-s" -> statsIntervalSeconds = Long.parseLong(value);
                        case "--allow" -> allowed.add(InetAddress.getByName(value));
                        case "--store" -> store = value;
                        case "--adm-r-max" -> admRMax = value.equals("auto") ? Double.NaN : Double.parseDouble(value);
                        case "--adm-tau-ms" -> admTauMs = Long.parseLong(value);
                        case "--adm-p-min" -> admPMin = Double.parseDouble(value);
                        case "--adm-p-max" -> admPMax = Double.parseDouble(value);
                        case "--adm-mode" -> admMode = SpeculationAdmission.Mode.valueOf(value.toUpperCase());
                        case "--adm-eta" -> admEta = Double.parseDouble(value);
                        case "--adm-width" -> admWidth = Double.parseDouble(value);
                        case "--adm-reach-cap" -> admReachCap = Integer.parseInt(value);
                        case "--adm-prior-mean" -> admPriorMean = Double.parseDouble(value);
                        case "--adm-prior-weight" -> admPriorWeight = Double.parseDouble(value);
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
            return new Options(port, shim, databases.split(","), Duration.ofMillis(lockTimeoutMs), deadlock,
                    prepareWaitDie, statsIntervalSeconds, allowed, store,
                    new SpeculationAdmission.Config(admMode, Duration.ofMillis(admTauMs), admRMax, admPMin,
                            admPMax != null ? admPMax : admMode == SpeculationAdmission.Mode.RATIO ? 0.95 : adm.pMax(),
                            admWidth, admReachCap, admEta, admPriorMean, admPriorWeight));
        }

        private static boolean parsePrepareDeadlock(String value) {
            return switch (value) {
                case "none" -> false;
                case "wait-die" -> true;
                default -> throw new IllegalArgumentException("unknown prepare deadlock policy " + value);
            };
        }

        private static DeadlockPolicy parseDeadlock(String value) {
            return switch (value) {
                case "wait-die" -> DeadlockPolicy.WAIT_DIE;
                case "wound-wait" -> DeadlockPolicy.WOUND_WAIT;
                case "none" -> DeadlockPolicy.NONE;
                default -> throw new IllegalArgumentException("unknown deadlock policy " + value);
            };
        }

        /** {@code database}'s store: the {@code --store} URL with {@code /database} appended (before any {@code ?}). */
        KvStore<TableKey<String>, String> newStore(String database) {
            if (store.equals("memory")) {
                return KvStores.fromUrl(store);
            }
            int query = store.indexOf('?');
            String server = query < 0 ? store : store.substring(0, query);
            String params = query < 0 ? "" : store.substring(query);
            return KvStores.fromUrl(server + "/" + database + params);
        }

        CoShim<TableKey<String>, String> newShim(KvStore<TableKey<String>, String> store) {
            return switch (shim) {
                case "speculative" -> new SpeculativeCoShim<>(store, lockTimeout,
                        SpeculativeCoShim.DEFAULT_TOMBSTONE_TTL, true, deadlock, prepareWaitDie);
                case "nonspeculative" -> new SpeculativeCoShim<>(store, lockTimeout,
                        SpeculativeCoShim.DEFAULT_TOMBSTONE_TTL, false, deadlock, prepareWaitDie);
                case "admission" -> new SpeculativeCoShim<>(store, lockTimeout,
                        SpeculativeCoShim.DEFAULT_TOMBSTONE_TTL, true, deadlock, prepareWaitDie, admission);
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
            if (store instanceof AutoCloseable closeable) {
                Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                    try {
                        closeable.close();
                    } catch (Exception e) {
                        System.err.println("closing the store of " + name + " failed: " + e);
                    }
                }, "coshim-store-close-" + name));
            }
            databases.put(name, options.newShim(store));
        }
        CoShimServer<TableKey<String>, String> server = new CoShimServer<>(
                databases, new TableKeyCodec<>(Codec.UTF8), Codec.UTF8, new InetSocketAddress(options.port()), token,
                options.allowed());
        System.out.println("coshim node (" + options.shim() + ", lock timeout " + options.lockTimeout().toMillis()
                + " ms, " + options.deadlock().name().toLowerCase().replace('_', '-')
                + (options.prepareWaitDie() ? ", prepare wait-die" : "")
                + (options.shim().equals("admission") ? ", " + options.admission() : "") + ") on port " + server.getPort() + ", databases " + databases.keySet()
                + ", store " + options.store()
                + (options.allowed().isEmpty() ? "" : ", allowed clients " + options.allowed()));

        Runnable printStats = () -> databases.forEach((name, shim) -> {
            if (shim instanceof SpeculativeCoShim<?, ?> speculative) {
                System.out.println("stats " + name + ": " + speculative.stats());
                System.out.println("speculation " + name + ": " + speculative.stats().speculationSummary());
                if (speculative.admissionSummary() != null) {
                    System.out.println("admission " + name + ": " + speculative.admissionSummary());
                }
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
