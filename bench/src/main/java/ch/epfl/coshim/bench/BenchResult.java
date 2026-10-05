package ch.epfl.coshim.bench;

import ch.epfl.coshim.core.ShimStats;
import ch.epfl.coshim.core.ShimStats.AbortCause;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Outcome of one {@link MicroBench} run. Counts and latencies cover the measurement window only; the
 * shim stats and the audit cover the whole run (warm-up included).
 *
 * @param committed           global transactions committed in the window
 * @param aborted             attempts rolled back in the window (shim abort or NO vote), then retried
 * @param timedOut            attempts rolled back by the global transaction timeout in the window
 * @param commitLatencyUs     latency of each committed attempt in the window (start of the attempt to the end
 *                            of phase 2), sorted ascending
 * @param shimStats           per shim, "a" then "b"
 * @param auditExpected       rmw only: committed txns (whole run) × branches × writes, else -1
 * @param auditActual         rmw only: sum of every value in both stores, else -1
 * @param warning             null, or why the run is not trustworthy (worker error, stuck worker)
 */
public record BenchResult(BenchConfig config, long committed, long aborted, long timedOut, List<Long> commitLatencyUs,
        List<ShimStats> shimStats, long auditExpected, long auditActual, String warning) {

    public double tps() {
        return committed * 1000.0 / config.measure().toMillis();
    }

    /** Share of attempts in the window that were rolled back (each is retried with the same plan). */
    public double abortRate() {
        long attempts = committed + aborted + timedOut;
        return attempts == 0 ? 0 : (double) (aborted + timedOut) / attempts;
    }

    public long latencyPercentileUs(int percentile) {
        if (commitLatencyUs.isEmpty()) {
            return -1;
        }
        return commitLatencyUs.get(Math.min(commitLatencyUs.size() - 1, commitLatencyUs.size() * percentile / 100));
    }

    public long meanLatencyUs() {
        return commitLatencyUs.isEmpty() ? -1
                : Math.round(commitLatencyUs.stream().mapToLong(Long::longValue).average().orElse(-1));
    }

    public long aborts(AbortCause cause) {
        return shimStats.stream().mapToLong(s -> s.aborts(cause)).sum();
    }

    public boolean audited() {
        return auditExpected >= 0;
    }

    public boolean auditOk() {
        return !audited() || auditExpected == auditActual;
    }

    /** No warning and, for rmw, the audit matched. */
    public boolean ok() {
        return warning == null && auditOk();
    }

    public String summary() {
        StringBuilder sb = new StringBuilder();
        sb.append(String.format(Locale.ROOT, "[%s] threads=%d tableSize=%d branches=%d reads=%d writes=%d skew=%.2f "
                        + "shimB=%d%% rmw=%b commitDelay=%dms lockTimeout=%dms txnTimeout=%dms branches %s "
                        + "measure=%ds%n",
                config.shim(), config.threads(), config.tableSize(), config.branches(), config.reads(), config.writes(),
                config.skewness(), config.shimBPercent(), config.rmw(), config.commitDelay().toMillis(),
                config.lockTimeout().toMillis(), config.txnTimeout().toMillis(),
                config.parallelBranches() ? "in parallel" : "in order", config.measure().toSeconds()));
        sb.append(String.format(Locale.ROOT, "  committed: %d (%.1f txn/s)   rolled back: %d (+%d by txn timeout), "
                        + "abort rate %.1f%%%n", committed, tps(), aborted, timedOut, 100 * abortRate()));
        sb.append(String.format(Locale.ROOT, "  commit latency: mean %d us, p50 %d us, p99 %d us, max %d us%n",
                meanLatencyUs(), latencyPercentileUs(50), latencyPercentileUs(99),
                commitLatencyUs.isEmpty() ? -1 : commitLatencyUs.get(commitLatencyUs.size() - 1)));
        sb.append("  shim a: ").append(shimStats.get(0)).append(System.lineSeparator());
        sb.append("  shim b: ").append(shimStats.get(1)).append(System.lineSeparator());
        sb.append("  audit: ").append(!audited() ? "n/a (blind writes)"
                : (auditOk() ? "OK" : "FAILED") + " (expected " + auditExpected + ", actual " + auditActual + ")");
        if (warning != null) {
            sb.append(System.lineSeparator()).append("  WARNING: ").append(warning);
        }
        return sb.toString();
    }

    public static String csvHeader() {
        return Stream.concat(Stream.of("label", "shim", "threads", "table_size", "branches", "reads", "writes", "skew",
                        "shim_b_percent", "rmw", "commit_delay_ms", "lock_timeout_ms", "txn_timeout_ms",
                        "parallel_branches", "measure_s", "committed", "tps",
                        "aborted", "timed_out", "abort_rate", "mean_us", "p50_us", "p99_us"),
                Stream.concat(Stream.of(AbortCause.values()).map(c -> "aborts_" + c.name().toLowerCase(Locale.ROOT)),
                        Stream.of("audit_ok")))
                .collect(Collectors.joining(","));
    }

    public String csvRow(String label) {
        return Stream.concat(Stream.of(label, config.shim(), config.threads(), config.tableSize(), config.branches(),
                                config.reads(), config.writes(), config.skewness(), config.shimBPercent(), config.rmw(),
                                config.commitDelay().toMillis(), config.lockTimeout().toMillis(),
                                config.txnTimeout().toMillis(), config.parallelBranches(),
                                config.measure().toSeconds(), committed, String.format(Locale.ROOT, "%.1f", tps()),
                                aborted, timedOut, String.format(Locale.ROOT, "%.4f", abortRate()),
                                meanLatencyUs(), latencyPercentileUs(50), latencyPercentileUs(99)),
                        Stream.concat(Stream.of(AbortCause.values()).map(this::aborts),
                                Stream.of(audited() ? auditOk() : "n/a")))
                .map(String::valueOf)
                .collect(Collectors.joining(","));
    }
}
