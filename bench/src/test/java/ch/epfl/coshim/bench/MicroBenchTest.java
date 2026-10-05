package ch.epfl.coshim.bench;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ch.epfl.coshim.core.ShimStats.AbortCause;
import java.time.Duration;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.jupiter.api.Test;

/**
 * Short, highly contended runs of the benchmark: an end-to-end concurrency test of the shim under
 * 2PC (lock waits, upgrades, cascades, timeouts, retries). The rmw audit must hold for both
 * variants: no lost update, no partial commit, no commit of rolled-back data.
 */
class MicroBenchTest {

    /** 20 hot keys per table, 8 workers, every txn writes 8 of them. */
    private static BenchConfig contended(String shim, boolean parallelBranches) {
        return new BenchConfig(shim, 8, 200, 2, 1, 2, 0.9, 50, true, Duration.ZERO, Duration.ofMillis(1500),
                Duration.ofMillis(1), Duration.ofMillis(200), Duration.ofMillis(500), parallelBranches);
    }

    private static BenchConfig contended(String shim) {
        return contended(shim, false);
    }

    @ParameterizedTest
    @ValueSource(strings = {"speculative", "nonspeculative"})
    void auditHoldsUnderContention(String shim) throws Exception {
        BenchResult result = new MicroBench(contended(shim)).run();

        assertNull(result.warning(), result.summary());
        assertTrue(result.committed() > 0, result.summary());
        assertTrue(result.audited());
        assertEquals(result.auditExpected(), result.auditActual(), result.summary());
        if (shim.equals("nonspeculative")) {
            assertEquals(0, result.aborts(AbortCause.CASCADE), "no speculation, nothing to cascade");
        }
    }

    /** Branches at the same time: cross-shim prepare cycles appear, the txn timeout must clear them. */
    @ParameterizedTest
    @ValueSource(strings = {"speculative", "nonspeculative"})
    void parallelBranchesRunAndKeepTheAudit(String shim) throws Exception {
        BenchResult result = new MicroBench(contended(shim, true)).run();

        assertNull(result.warning(), result.summary());
        assertTrue(result.committed() > 0, result.summary());
        assertEquals(result.auditExpected(), result.auditActual(), result.summary());
    }

    @Test
    void blindWritesHaveNoAuditButRun() throws Exception {
        BenchConfig c = contended("speculative");
        BenchConfig blind = new BenchConfig(c.shim(), c.threads(), c.tableSize(), c.branches(), c.reads(), c.writes(),
                c.skewness(), c.shimBPercent(), false, c.warmup(), Duration.ofMillis(500), c.commitDelay(),
                c.lockTimeout(), c.txnTimeout(), false);
        BenchResult result = new MicroBench(blind).run();
        assertNull(result.warning(), result.summary());
        assertTrue(result.committed() > 0);
        assertTrue(result.ok());
    }

    @Test
    void parseRejectsImpossibleWorkloads() {
        assertThrows(IllegalArgumentException.class, () -> BenchConfig.parse(new String[] {"--shim", "nocc"}));
        assertThrows(IllegalArgumentException.class,
                () -> BenchConfig.parse(new String[] {"--table-size", "10", "--skew", "0.9"}));
        assertEquals(64, BenchConfig.parse(new String[] {"--threads", "64", "--csv", "x.csv"}).threads());
    }
}
