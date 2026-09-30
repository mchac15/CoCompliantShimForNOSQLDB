package ch.epfl.coshim.net;

import ch.epfl.coshim.core.CoShim;
import ch.epfl.coshim.core.NoCcShim;
import ch.epfl.coshim.core.SpeculativeCoShim;
import ch.epfl.coshim.store.InMemoryKvStore;
import ch.epfl.coshim.store.KvStore;
import ch.epfl.coshim.store.TableKey;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * Runs one shim node hosting one or more databases, one shim each (String keys and values, one
 * in-memory store per database for now).
 *
 * <pre>
 *   COSHIM_TOKEN=secret java -cp ... ch.epfl.coshim.net.CoShimNode [port] [shim] [databases] [allowed-client ...]
 *     port            default 7000
 *     shim            speculative (pseudo.txt, TODO) | nocc (no concurrency control, baseline); default nocc
 *     databases       comma-separated database names; default "default"
 *     allowed-client  optional client addresses (e.g. the RMs' hosts); default: any authenticated client
 * </pre>
 *
 * RMs then use one data source per database:
 * {@code new CoShimDataSource<>("host:7000/orders", new RemoteCoShim<>(node, "orders", ...))}.
 */
public final class CoShimNode {

    private CoShimNode() {}

    public static void main(String[] args) throws Exception {
        String token = System.getenv("COSHIM_TOKEN");
        if (token == null || token.isEmpty()) {
            System.err.println("set COSHIM_TOKEN: the shared secret the RMs must present");
            System.exit(2);
        }
        int port = args.length > 0 ? Integer.parseInt(args[0]) : 7000;
        String kind = args.length > 1 ? args[1] : "nocc";
        // A comma-separated list, e.g. "shop,billing": the node hosts one database (and one shim) per
        // name. With no argument it hosts a single database called "default".
        String[] databaseNames = (args.length > 2 ? args[2] : "default").split(",");
        Set<InetAddress> allowed = new HashSet<>();
        for (int i = 3; i < args.length; i++) {
            allowed.add(InetAddress.getByName(args[i]));
        }

        Map<String, CoShim<TableKey<String>, String>> databases = new LinkedHashMap<>();
        for (String name : databaseNames) {
            KvStore<TableKey<String>, String> store = new InMemoryKvStore<>();
            databases.put(name, switch (kind) {
                case "speculative" -> new SpeculativeCoShim<>(store, Duration.ofMillis(200));
                case "nocc" -> new NoCcShim<>(store);
                default -> throw new IllegalArgumentException("unknown shim " + kind);
            });
        }
        CoShimServer<TableKey<String>, String> server = new CoShimServer<>(
                databases, new TableKeyCodec<>(Codec.UTF8), Codec.UTF8, new InetSocketAddress(port), token, allowed);
        System.out.println("coshim node (" + kind + ") on port " + server.getPort() + ", databases " + databases.keySet()
                + (allowed.isEmpty() ? "" : ", allowed clients " + allowed));
        Thread.currentThread().join();
    }
}
