package ch.epfl.coshim.bench;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
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
 * @param auditActual       rmw only: sum of every value on every participant, else -1
 * @param branchFailures    failed branches in the window by cause (deadlock, lock_timeout,
 *                          serialization, xa_rollback, other:...); a TC timeout fails none
 * @param txns              every txn (plan) that ended in the window, with its retries; plus those
 *                          still retrying when the window closed (UNFINISHED)
 * @param warning           null, or why the run is not trustworthy (worker error, stuck worker)
 */
public record BenchResult(BenchConfig config, long committed, long aborted, long timedOut, long unknown,
        List<Long> commitLatencyUs, long auditExpectedMin, long auditExpectedMax, long auditActual,
        Map<String, Long> branchFailures, List<Txn> txns, String warning) {

    /**
     * One txn as the application saw it: all its attempts with the same plan.
     *
     * @param attempts   1 = committed at the first try
     * @param outcome    COMMITTED, UNKNOWN or UNFINISHED
     * @param causes     why each failed attempt failed (branch failure causes, tc_timeout, aborted)
     * @param latencyUs  from the first attempt's begin to the last attempt's end
     */
    public record Txn(int attempts, int reads, int writes, int branches, String outcome, List<String> causes,
            long latencyUs) {

        static Txn of(List<MicroBench.Branch> plan, int attempts, String outcome, List<String> causes,
                long latencyUs) {
            return new Txn(attempts, plan.stream().mapToInt(b -> b.readKeys().size()).sum(),
                    plan.stream().mapToInt(b -> b.writeKeys().size()).sum(), plan.size(), outcome,
                    List.copyOf(causes), latencyUs);
        }

        int ops() {
            return reads + writes;
        }
    }

    /** Buckets of the retries histogram: 0, 1, 2, 3, 4 or more retries. */
    private static final int RETRY_BUCKETS = 5;

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

    /** Mean attempts per txn (1 = no retries). */
    public double meanAttempts() {
        return txns.isEmpty() ? 0 : txns.stream().mapToInt(Txn::attempts).average().orElse(0);
    }

    /** Share of the txns that needed at least one retry (or never committed). */
    public double retriedShare() {
        return txns.isEmpty() ? 0 : (double) txns.stream().filter(t -> t.attempts() > 1 || isUnfinished(t)).count()
                / txns.size();
    }

    private static boolean isUnfinished(Txn t) {
        return t.outcome().equals("UNFINISHED");
    }

    /** "0:n 1:n ... 4+:n": how many txns needed that many retries. */
    String retriesText() {
        long[] h = new long[RETRY_BUCKETS];
        txns.forEach(t -> h[Math.min(t.attempts() - 1, RETRY_BUCKETS - 1)]++);
        List<String> parts = new ArrayList<>();
        for (int i = 0; i < RETRY_BUCKETS; i++) {
            parts.add(i + (i == RETRY_BUCKETS - 1 ? "+" : "") + ":" + h[i]);
        }
        return String.join(" ", parts);
    }

    /**
     * Per total ops of a txn (reads + writes over its branches, by 4): txns, share retried, mean
     * attempts. Shows whether bigger txns are the ones that abort.
     */
    String retriesByOpsText() {
        Map<Integer, List<Txn>> byOps = new TreeMap<>();
        txns.forEach(t -> byOps.computeIfAbsent(t.ops() / 4 * 4, k -> new ArrayList<>()).add(t));
        return byOps.entrySet().stream().map(e -> {
            List<Txn> ts = e.getValue();
            long retried = ts.stream().filter(t -> t.attempts() > 1 || isUnfinished(t)).count();
            return String.format(Locale.ROOT, "%d-%d:%d/%.0f%%/%.2f", e.getKey(), e.getKey() + 3, ts.size(),
                    100.0 * retried / ts.size(), ts.stream().mapToInt(Txn::attempts).average().orElse(0));
        }).collect(Collectors.joining(" "));
    }

    /** Mean reads and writes of the txns that committed at once vs those that were retried. */
    String opsOfRetriedText() {
        List<Txn> once = txns.stream().filter(t -> t.attempts() == 1 && !isUnfinished(t)).toList();
        List<Txn> retried = txns.stream().filter(t -> t.attempts() > 1 || isUnfinished(t)).toList();
        return String.format(Locale.ROOT, "first try %d txns (%.1f r, %.1f w), retried %d txns (%.1f r, %.1f w)",
                once.size(), once.stream().mapToInt(Txn::reads).average().orElse(0),
                once.stream().mapToInt(Txn::writes).average().orElse(0), retried.size(),
                retried.stream().mapToInt(Txn::reads).average().orElse(0),
                retried.stream().mapToInt(Txn::writes).average().orElse(0));
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
        if (config.opsMean() > 0 || config.thinkMeanUs() > 0) {
            sb.append(String.format(Locale.ROOT, "  ops per branch: %s   think time: %s%n",
                    config.opsMean() > 0 ? String.format(Locale.ROOT, "N(%.1f, %.1f) in [0, %d]", config.opsMean(),
                            config.opsSd(), config.opsMax()) : config.reads() + " r + " + config.writes() + " w",
                    config.thinkMeanUs() > 0 ? String.format(Locale.ROOT, "N(%.0f, %.0f) us", config.thinkMeanUs(),
                            config.thinkSdUs()) : "none"));
        }
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
        sb.append(System.lineSeparator()).append(String.format(Locale.ROOT,
                "  txns: %d, mean attempts %.2f, retried %.1f%%; retries %s", txns.size(), meanAttempts(),
                100 * retriedShare(), retriesText()));
        sb.append(System.lineSeparator()).append("  by ops (txns/retried/mean attempts): ").append(retriesByOpsText());
        sb.append(System.lineSeparator()).append("  ").append(opsOfRetriedText());
        sb.append(System.lineSeparator()).append("  failed branches: ")
                .append(branchFailures.isEmpty() ? "none" : failuresText(", "))
                .append(" (shim abort causes: see the shim nodes' logs)");
        if (warning != null) {
            sb.append(System.lineSeparator()).append("  WARNING: ").append(warning);
        }
        return sb.toString();
    }

    public static String csvHeader() {
        return Stream.of("label", "variant", "shims", "threads", "table_size", "branches", "reads", "writes", "skew",
                        "rmw", "txn_timeout_ms", "parallel_branches", "measure_s", "committed", "tps", "aborted",
                        "timed_out", "unknown", "abort_rate", "mean_us", "p50_us", "p99_us", "audit_ok",
                        "branch_failures", "ops_mean", "ops_sd", "ops_max", "think_mean_us", "think_sd_us",
                        "mean_attempts", "retried_share")
                .collect(Collectors.joining(","));
    }

    public String csvRow(String label) {
        return Stream.of(label, config.variant(), config.shims().size(), config.threads(), config.tableSize(),
                        config.branches(), config.reads(), config.writes(), config.skewness(), config.rmw(),
                        config.txnTimeout().toMillis(), config.parallelBranches(), config.measure().toSeconds(),
                        committed, String.format(Locale.ROOT, "%.1f", tps()), aborted, timedOut, unknown,
                        String.format(Locale.ROOT, "%.4f", abortRate()), meanLatencyUs(), latencyPercentileUs(50),
                        latencyPercentileUs(99), audited() ? auditOk() : "n/a", failuresText(";"), config.opsMean(),
                        config.opsSd(), config.opsMax(), config.thinkMeanUs(), config.thinkSdUs(),
                        String.format(Locale.ROOT, "%.3f", meanAttempts()),
                        String.format(Locale.ROOT, "%.4f", retriedShare()))
                .map(String::valueOf)
                .collect(Collectors.joining(","));
    }

    public static String txnCsvHeader() {
        return "label,variant,skew,attempts,reads,writes,ops,branches,outcome,first_cause,causes,latency_us";
    }

    /** One row per txn, for {@code <csv>-txns.csv}. */
    public List<String> txnRows(String label) {
        return txns.stream().map(t -> Stream.of(label, config.variant(), config.skewness(), t.attempts(), t.reads(),
                        t.writes(), t.ops(), t.branches(), t.outcome(), t.causes().isEmpty() ? "" : t.causes().get(0),
                        String.join(";", t.causes()), t.latencyUs())
                .map(String::valueOf).collect(Collectors.joining(","))).toList();
    }

    private String failuresText(String separator) {
        return branchFailures.entrySet().stream().map(e -> e.getKey() + "=" + e.getValue())
                .collect(Collectors.joining(separator));
    }
}
