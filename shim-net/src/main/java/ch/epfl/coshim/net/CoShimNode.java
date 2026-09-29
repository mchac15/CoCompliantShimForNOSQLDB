package ch.epfl.coshim.net;

import ch.epfl.coshim.core.CoShim;
import ch.epfl.coshim.core.NoCcShim;
import ch.epfl.coshim.core.SpeculativeCoShim;
import ch.epfl.coshim.store.InMemoryKvStore;
import ch.epfl.coshim.store.KvStore;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.HashSet;
import java.util.Set;

/**
 * Runs one shim node (String keys and values, in-memory store for now).
 *
 * <pre>
 *   COSHIM_TOKEN=secret java -cp ... ch.epfl.coshim.net.CoShimNode [port] [shim] [allowed-client ...]
 *     port            default 7000
 *     shim            speculative (pseudo.txt, TODO) | nocc (no concurrency control, baseline); default nocc
 *     allowed-client  optional client addresses (e.g. the RMs' hosts); default: any authenticated client
 * </pre>
 *
 * RMs then use {@code new CoShimDataSource<>("host:7000/orders-kv", new RemoteCoShim<>(...))}.
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
        Set<InetAddress> allowed = new HashSet<>();
        for (int i = 2; i < args.length; i++) {
            allowed.add(InetAddress.getByName(args[i]));
        }

        KvStore<String, String> store = new InMemoryKvStore<>();
        CoShim<String, String> shim = switch (kind) {
            case "speculative" -> new SpeculativeCoShim<>(store, Duration.ofMillis(200));
            case "nocc" -> new NoCcShim<>(store);
            default -> throw new IllegalArgumentException("unknown shim " + kind);
        };
        CoShimServer<String, String> server = new CoShimServer<>(
                shim, Codec.UTF8, Codec.UTF8, new InetSocketAddress(port), token, allowed);
        System.out.println("coshim node (" + kind + ") listening on port " + server.getPort()
                + (allowed.isEmpty() ? "" : ", allowed clients " + allowed));
        Thread.currentThread().join();
    }
}
