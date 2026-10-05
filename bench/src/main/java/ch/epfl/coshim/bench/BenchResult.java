package ch.epfl.coshim.bench;

import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Outcome of one {@link MicroBench} run. Counts and latencies cover the measurement window only; the
 * audit covers the whole run (warm-up included). The shims' abort causes are in the nodes' logs.
 *
 * @param committed         global transactions committed in the window
 * @param aborted           attempts rolled back in the window (shim abort, NO vote, refused branch),
 *                          then retried with the same plan
 * @param timedOut          attempts rolled back by the TC's global timeout in the window, then retried
 * @param unknown           attempts in the window whose commit got no clear answer (not retried)
 * @param commitLatencyUs   latency of each committed attempt in the window (TM begin to the end of TM
 *                          commit), sorted ascending
 * @param auditExpectedMin  rmw only: committed txns (whole run) × branches × writes, else -1
 * @param auditExpectedMax  rmw only: the same counting the unknown outcomes as committed, else -1
 * @param auditActual       rmw only: sum of every value on every shim, else -1
 * @param warning           null, or why the run is not trustworthy (worker error, stuck worker)
 */
public record BenchResult(BenchConfig config, long committed, long aborted, long timedOut, long unknown,
        List<Long> commitLatencyUs, long auditExpectedMin, long auditExpectedMax, long auditActual, String warning) {

    public double tps() {
        return committed * 1000.0 / config.measure().toMillis();
    }

    /** Share of attempts in the window that were rolled back (each is retried with the same plan). */
    public double abortRate() {
        long attempts = committed + aborted + timedOut + unknown;
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

    public boolean audited() {
        return auditExpectedMin >= 0;
    }

    public boolean auditOk() {
        return !audited() || (auditActual >= auditExpectedMin && auditActual <= auditExpectedMax);
    }

    /** No warning and, for rmw, the audit matched. */
    public boolean ok() {
        return warning == null && auditOk();
    }

    public String summary() {
        StringBuilder sb = new StringBuilder();
        sb.append(String.format(Locale.ROOT, "[%s] shims=%s tc=%s threads=%d tableSize=%d branches=%d reads=%d "
                        + "writes=%d skew=%.2f rmw=%b txnTimeout=%dms branches %s measure=%ds%n",
                config.variant(), String.join(",", config.shims()), config.tc(), config.threads(), config.tableSize(),
                config.branches(), config.reads(), config.writes(), config.skewness(), config.rmw(),
                config.txnTimeout().toMillis(), config.parallelBranches() ? "in parallel" : "in order",
                config.measure().toSeconds()));
        sb.append(String.format(Locale.ROOT, "  committed: %d (%.1f txn/s)   rolled back: %d (+%d by TC timeout), "
                        + "unknown: %d, abort rate %.1f%%%n",
                committed, tps(), aborted, timedOut, unknown, 100 * abortRate()));
        sb.append(String.format(Locale.ROOT, "  commit latency: mean %d us, p50 %d us, p99 %d us, max %d us%n",
                meanLatencyUs(), latencyPercentileUs(50), latencyPercentileUs(99),
                commitLatencyUs.isEmpty() ? -1 : commitLatencyUs.get(commitLatencyUs.size() - 1)));
        sb.append("  audit: ").append(!audited() ? "n/a (blind writes or failed run)"
                : (auditOk() ? "OK" : "FAILED") + " (expected " + auditExpectedMin
                        + (auditExpectedMax > auditExpectedMin ? ".." + auditExpectedMax : "")
                        + ", actual " + auditActual + ")");
        sb.append(System.lineSeparator()).append("  shim abort causes: see the shim nodes' logs");
        if (warning != null) {
            sb.append(System.lineSeparator()).append("  WARNING: ").append(warning);
        }
        return sb.toString();
    }

    public static String csvHeader() {
        return Stream.of("label", "variant", "shims", "threads", "table_size", "branches", "reads", "writes", "skew",
                        "rmw", "txn_timeout_ms", "parallel_branches", "measure_s", "committed", "tps", "aborted",
                        "timed_out", "unknown", "abort_rate", "mean_us", "p50_us", "p99_us", "audit_ok")
                .collect(Collectors.joining(","));
    }

    public String csvRow(String label) {
        return Stream.of(label, config.variant(), config.shims().size(), config.threads(), config.tableSize(),
                        config.branches(), config.reads(), config.writes(), config.skewness(), config.rmw(),
                        config.txnTimeout().toMillis(), config.parallelBranches(), config.measure().toSeconds(),
                        committed, String.format(Locale.ROOT, "%.1f", tps()), aborted, timedOut, unknown,
                        String.format(Locale.ROOT, "%.4f", abortRate()), meanLatencyUs(), latencyPercentileUs(50),
                        latencyPercentileUs(99), audited() ? auditOk() : "n/a")
                .map(String::valueOf)
                .collect(Collectors.joining(","));
    }
}
