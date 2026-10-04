package ch.epfl.coshim.bench;

import java.time.Duration;

/**
 * Parameters of one {@link MicroBench} run. The defaults are the "simple test": Acta's Micro workload
 * with read-modify-write writes, skew 0.9, 50 workers, 2 branches per global transaction.
 *
 * @param shim                 speculative | nonspeculative
 * @param threads              closed-loop workers (Acta's concurrency)
 * @param tableSize            keys per table; branch i of every txn uses table micro-i
 * @param branches             branches (subtransactions) per global transaction
 * @param reads                pure reads per branch, done first
 * @param writes               writes per branch, done after the reads
 * @param skewness             share of each branch's keys drawn from the hot range, the first
 *                             (1 - skewness) * tableSize keys (Acta's definition, in [0.5, 1))
 * @param shimBPercent         chance that a branch goes to shim "b" instead of "a" (Acta's pgPercentage)
 * @param rmw                  writes are v := get(k) + 1 instead of blind random values; enables the audit
 * @param warmup               run time before measuring
 * @param measure              measured run time
 * @param commitDelay          emulated TM → TC → RM round trip between the last YES vote and phase 2
 * @param lockTimeout          the shims' lock_timeout
 * @param txnTimeout           global transaction timeout, as Seata's TC: rolls a stuck txn back
 */
public record BenchConfig(String shim, int threads, int tableSize, int branches, int reads, int writes,
        double skewness, int shimBPercent, boolean rmw, Duration warmup, Duration measure, Duration commitDelay,
        Duration lockTimeout, Duration txnTimeout) {

    public static BenchConfig defaults() {
        return new BenchConfig("speculative", 50, 10_000, 2, 2, 2, 0.9, 50, true, Duration.ofSeconds(10),
                Duration.ofSeconds(30), Duration.ofMillis(1), Duration.ofSeconds(1), Duration.ofSeconds(10));
    }

    public boolean speculative() {
        return switch (shim) {
            case "speculative" -> true;
            case "nonspeculative" -> false;
            default -> throw new IllegalArgumentException("--shim must be speculative or nonspeculative: " + shim);
        };
    }

    /** Keys per table in the hot range. */
    public int hotRangeSize() {
        return (int) (tableSize * (1 - skewness));
    }

    /** Same checks as Acta's Micro run endpoint: every txn must find distinct hot and cold keys. */
    public BenchConfig validate() {
        speculative();
        require(threads > 0, "--threads must be > 0");
        require(tableSize > 0, "--table-size must be > 0");
        require(branches > 0, "--branches must be > 0");
        require(reads >= 0 && writes >= 0, "--reads and --writes must be >= 0");
        require(reads + writes > 0, "a branch needs at least one read or write");
        require(skewness >= 0.5 && skewness < 1.0, "--skew must be in [0.5, 1)");
        require(shimBPercent >= 0 && shimBPercent <= 100, "--shim-b-percent must be in [0, 100]");
        require(!measure.isZero() && !measure.isNegative(), "--measure-s must be > 0");
        require(!warmup.isNegative() && !commitDelay.isNegative(), "durations must be >= 0");
        long hotPerBranch = (long) Math.ceil(reads * skewness) + (long) Math.ceil(writes * skewness);
        long coldPerBranch = (long) reads + writes - hotPerBranch;
        require(hotPerBranch * branches <= hotRangeSize(), "requested hot keys exceed the hot range");
        require(coldPerBranch * branches <= tableSize - hotRangeSize(), "requested cold keys exceed the cold range");
        return this;
    }

    private static void require(boolean condition, String message) {
        if (!condition) {
            throw new IllegalArgumentException(message);
        }
    }

    /** Parses {@code --flag value} pairs over the defaults; unknown flags are an error. */
    public static BenchConfig parse(String[] args) {
        BenchConfig c = defaults();
        String shim = c.shim;
        int threads = c.threads;
        int tableSize = c.tableSize;
        int branches = c.branches;
        int reads = c.reads;
        int writes = c.writes;
        double skewness = c.skewness;
        int shimBPercent = c.shimBPercent;
        boolean rmw = c.rmw;
        Duration warmup = c.warmup;
        Duration measure = c.measure;
        Duration commitDelay = c.commitDelay;
        Duration lockTimeout = c.lockTimeout;
        Duration txnTimeout = c.txnTimeout;
        for (int i = 0; i < args.length; i++) {
            String flag = args[i];
            if (flag.equals("--csv") || flag.equals("--label")) {
                i++;   // handled by MicroBench.main
                continue;
            }
            if (i + 1 >= args.length) {
                throw new IllegalArgumentException(flag + " needs a value");
            }
            String v = args[++i];
            switch (flag) {
                case "--shim" -> shim = v;
                case "--threads" -> threads = Integer.parseInt(v);
                case "--table-size" -> tableSize = Integer.parseInt(v);
                case "--branches" -> branches = Integer.parseInt(v);
                case "--reads" -> reads = Integer.parseInt(v);
                case "--writes" -> writes = Integer.parseInt(v);
                case "--skew" -> skewness = Double.parseDouble(v);
                case "--shim-b-percent" -> shimBPercent = Integer.parseInt(v);
                case "--rmw" -> rmw = Boolean.parseBoolean(v);
                case "--warmup-s" -> warmup = seconds(v);
                case "--measure-s" -> measure = seconds(v);
                case "--commit-delay-ms" -> commitDelay = Duration.ofMillis(Long.parseLong(v));
                case "--lock-timeout-ms" -> lockTimeout = Duration.ofMillis(Long.parseLong(v));
                case "--txn-timeout-ms" -> txnTimeout = Duration.ofMillis(Long.parseLong(v));
                default -> throw new IllegalArgumentException("unknown option " + flag);
            }
        }
        return new BenchConfig(shim, threads, tableSize, branches, reads, writes, skewness, shimBPercent, rmw, warmup,
                measure, commitDelay, lockTimeout, txnTimeout).validate();
    }

    private static Duration seconds(String v) {
        return Duration.ofMillis(Math.round(Double.parseDouble(v) * 1000));
    }
}
