package ch.epfl.coshim.bench;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import ch.epfl.coshim.core.CoShim;
import ch.epfl.coshim.core.SpeculativeCoShim;
import ch.epfl.coshim.net.CoShimServer;
import ch.epfl.coshim.net.Codec;
import ch.epfl.coshim.net.TableKeyCodec;
import ch.epfl.coshim.store.InMemoryKvStore;
import ch.epfl.coshim.store.TableKey;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.sql.SQLException;
import java.sql.SQLTransactionRollbackException;
import java.time.Duration;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.transaction.xa.XAException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/**
 * The workload rules, and short highly contended runs through the real path: a Seata TC, and two
 * shim databases served over TCP by an in-JVM {@link CoShimServer} (what {@code CoShimNode} runs).
 * The runs need a TC on {@code 127.0.0.1:8091} (or {@code -Dbench.tc=host:port}) and are skipped
 * when none is reachable; see README.md for starting one.
 */
class MicroBenchTest {

    private static final String TOKEN = "bench-test";

    private static BenchConfig config(String... args) {
        return BenchConfig.parse(args);
    }

    @Test
    void plansFollowActasRules() {
        BenchConfig c = config("--table-size", "1000", "--branches", "3", "--reads", "2", "--writes", "3",
                "--skew", "0.9");
        for (int i = 0; i < 1000; i++) {
            List<MicroBench.Branch> plan = MicroBench.randomPlan(c, 4);
            assertEquals(3, plan.size());
            Set<Integer> keys = new HashSet<>();
            for (int b = 0; b < plan.size(); b++) {
                MicroBench.Branch branch = plan.get(b);
                assertEquals("micro-" + b, branch.table());
                assertTrue(branch.shim() >= 0 && branch.shim() < 4);
                assertEquals(2, branch.readKeys().size());
                assertEquals(3, branch.writeKeys().size());
                branch.readKeys().forEach(k -> assertTrue(keys.add(k), "keys are distinct in a txn"));
                branch.writeKeys().forEach(k -> assertTrue(keys.add(k), "keys are distinct in a txn"));
            }
            // ceil(2 * 0.9) + ceil(3 * 0.9) = 5 hot keys per branch, i.e. in [0, 100)
            assertEquals(15, keys.stream().filter(k -> k < 100).count());
        }
    }

    @Test
    void parseRejectsImpossibleWorkloads() {
        assertThrows(IllegalArgumentException.class, () -> config("--table-size", "10", "--skew", "0.9"));
        assertThrows(IllegalArgumentException.class, () -> config("--shims", "localhost:7000"));
        assertThrows(IllegalArgumentException.class, () -> config("--tc", "nohost"));
        assertThrows(IllegalArgumentException.class, () -> config("--bogus", "1"));
        BenchConfig c = config("--threads", "64", "--shims", "h1:7000/a, h2:7001/b,h3:7002/c", "--csv", "x.csv");
        assertEquals(64, c.threads());
        assertEquals(List.of("h1:7000/a", "h2:7001/b", "h3:7002/c"), c.shims());
    }

    @Test
    void parseAcceptsSonataDatabases() {
        BenchConfig c = config("--shims", "jdbc:mysql://h1:3306/acta,jdbc:postgresql://h2:5432/acta?user=x",
                "--sonata", "true");
        assertTrue(c.sonata());
        assertEquals(List.of("jdbc:mysql://h1:3306/acta", "jdbc:postgresql://h2:5432/acta?user=x"), c.shims());
        assertTrue(Participant.isCoShim("h1:7000/a"));
        assertTrue(!Participant.isCoShim("jdbc:mysql://h1:3306/acta"));
        assertThrows(IllegalArgumentException.class, () -> config("--shims", "jdbc:oracle:thin:@h:1521/x"));
    }

    @Test
    void branchFailuresAreClassified() {
        SQLException mysqlDeadlock = new SQLTransactionRollbackException("Deadlock found", "40001", 1213);
        assertEquals("deadlock", MicroBench.failureCause(new SQLException("wrapped", mysqlDeadlock)));
        assertEquals("lock_timeout", MicroBench.failureCause(new SQLException("Lock wait timeout", "HY000", 1205)));
        assertEquals("serialization", MicroBench.failureCause(new SQLException("could not serialize", "40001")));
        assertEquals("deadlock", MicroBench.failureCause(new SQLException("deadlock detected", "40P01")));
        assertEquals("serialization", MicroBench.failureCause(new SQLException("prepare failed",
                new XAException("PostgreSQL dummy write of global txn branch (x) failed due to serialization failure"))));
        assertEquals("xa_rollback", MicroBench.failureCause(new SQLException("prepare failed",
                new XAException(XAException.XA_RBROLLBACK))));
        assertEquals("shim_abort", MicroBench.failureCause(new SQLTransactionRollbackException("aborted")));
        assertEquals("other:IllegalStateException", MicroBench.failureCause(new SQLException(new IllegalStateException())));
    }

    /** 20 hot keys per table, 8 workers, every txn writes 4 of them; both variants, both branch modes. */
    @ParameterizedTest
    @CsvSource({"true,false", "true,true", "false,false", "false,true"})
    void auditHoldsThroughTheTcAndTcp(boolean speculative, boolean parallelBranches) throws Exception {
        String tc = System.getProperty("bench.tc", "127.0.0.1:8091");
        assumeTrue(reachable(tc), "no Seata TC at " + tc);

        Map<String, CoShim<TableKey<String>, String>> databases = new LinkedHashMap<>();
        for (String db : List.of("a", "b")) {
            databases.put(db, new SpeculativeCoShim<>(new InMemoryKvStore<>(), Duration.ofMillis(200),
                    SpeculativeCoShim.DEFAULT_TOMBSTONE_TTL, speculative));
        }
        try (CoShimServer<TableKey<String>, String> node = new CoShimServer<>(databases,
                new TableKeyCodec<>(Codec.UTF8), Codec.UTF8, new InetSocketAddress(InetAddress.getLoopbackAddress(), 0),
                TOKEN, Set.of())) {
            String host = "127.0.0.1:" + node.getPort();
            BenchConfig c = config("--variant", speculative ? "speculative" : "nonspeculative",
                    "--shims", host + "/a," + host + "/b", "--tc", tc, "--threads", "8", "--table-size", "200",
                    "--branches", "2", "--reads", "1", "--writes", "2", "--skew", "0.9", "--warmup-s", "0",
                    "--measure-s", "2", "--txn-timeout-ms", "1000", "--parallel-branches", String.valueOf(parallelBranches));
            BenchResult result;
            try (MicroBench bench = new MicroBench(c, TOKEN)) {
                result = bench.run();
            }
            assertNull(result.warning(), result.summary());
            assertTrue(result.committed() > 0, result.summary());
            assertTrue(result.audited(), result.summary());
            assertTrue(result.auditOk(), result.summary());
        }
    }

    private static boolean reachable(String hostPort) {
        int colon = hostPort.indexOf(':');
        try (Socket s = new Socket()) {
            s.connect(new InetSocketAddress(hostPort.substring(0, colon), Integer.parseInt(hostPort.substring(colon + 1))),
                    500);
            return true;
        } catch (Exception e) {
            return false;
        }
    }
}
