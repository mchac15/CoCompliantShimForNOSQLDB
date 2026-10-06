package ch.epfl.coshim.bench;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;

/**
 * Parameters of one {@link MicroBench} run. The defaults are the "simple test": Acta's Micro workload
 * with read-modify-write writes, skew 0.9, 50 workers, 2 branches per global transaction.
 *
 * <p>The shim variant (speculative or not) and its lock timeout are set on the shim nodes
 * ({@code CoShimNode --shim ... --lock-timeout-ms ...}), not here: the benchmark only reaches them
 * over TCP.
 *
 * @param variant          label of the run in the results (the variant the nodes were started with)
 * @param shims            the participants: shim databases ({@code host:port/database}) or SQL databases
 *                         ({@code jdbc:mysql://...}, {@code jdbc:postgresql://...}); each branch goes to
 *                         one of them at random
 * @param tc               the Seata TC, {@code host:port}
 * @param threads          closed-loop workers (Acta's concurrency)
 * @param tableSize        keys per table; branch i of every txn uses table micro-i
 * @param branches         branches (subtransactions) per global transaction
 * @param reads            pure reads per branch, done first
 * @param writes           writes per branch, done after the reads
 * @param skewness         share of each branch's keys drawn from the hot range, the first
 *                         (1 - skewness) * tableSize keys (Acta's definition, in [0.5, 1))
 * @param rmw              writes are v := get(k) + 1 instead of blind random values; enables the audit
 * @param warmup           run time before measuring
 * @param measure          measured run time
 * @param txnTimeout       global transaction timeout, enforced by the TC (rolls the branches back)
 * @param parallelBranches run the branches of a global transaction at the same time (each still
 *                         start, ops, end, prepare) instead of one after the other
 * @param sonata           turn Sonata on for the SQL databases ({@code sonata.enableGlobalSerializability};
 *                         Sonata's other settings stay at their defaults). Seata reads it once per JVM
 */
public record BenchConfig(String variant, List<String> shims, String tc, int threads, int tableSize, int branches,
        int reads, int writes, double skewness, boolean rmw, Duration warmup, Duration measure, Duration txnTimeout,
        boolean parallelBranches, boolean sonata) {

    public static BenchConfig defaults() {
        return new BenchConfig("unknown", List.of("127.0.0.1:7000/a", "127.0.0.1:7001/b"), "127.0.0.1:8091", 50,
                10_000, 2, 2, 2, 0.9, true, Duration.ofSeconds(10), Duration.ofSeconds(30), Duration.ofSeconds(10),
                false, false);
    }

    /** Keys per table in the hot range. */
    public int hotRangeSize() {
        return (int) (tableSize * (1 - skewness));
    }

    /** Same checks as Acta's Micro run endpoint: every txn must find distinct hot and cold keys. */
    public BenchConfig validate() {
        require(!shims.isEmpty(), "--shims needs at least one host:port/database or jdbc: URL");
        for (String shim : shims) {
            require(shim.matches("[^:/]+:\\d+/.+") || shim.matches("jdbc:(mysql|postgresql)://.+"),
                    "--shims entries are host:port/database, jdbc:mysql://... or jdbc:postgresql://...: " + shim);
        }
        require(tc.matches("[^:]+:\\d+"), "--tc is host:port: " + tc);
        require(threads > 0, "--threads must be > 0");
        require(tableSize > 0, "--table-size must be > 0");
        require(branches > 0, "--branches must be > 0");
        require(reads >= 0 && writes >= 0, "--reads and --writes must be >= 0");
        require(reads + writes > 0, "a branch needs at least one read or write");
        require(skewness >= 0.5 && skewness < 1.0, "--skew must be in [0.5, 1)");
        require(!measure.isZero() && !measure.isNegative(), "--measure-s must be > 0");
        require(!warmup.isNegative(), "--warmup-s must be >= 0");
        require(txnTimeout.toMillis() >= 1 && txnTimeout.toMillis() <= Integer.MAX_VALUE,
                "--txn-timeout-ms must be in [1, 2^31)");
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
        String variant = c.variant;
        List<String> shims = c.shims;
        String tc = c.tc;
        int threads = c.threads;
        int tableSize = c.tableSize;
        int branches = c.branches;
        int reads = c.reads;
        int writes = c.writes;
        double skewness = c.skewness;
        boolean rmw = c.rmw;
        Duration warmup = c.warmup;
        Duration measure = c.measure;
        Duration txnTimeout = c.txnTimeout;
        boolean parallelBranches = c.parallelBranches;
        boolean sonata = c.sonata;
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
                case "--variant" -> variant = v;
                case "--shims" -> shims = Arrays.stream(v.split(",")).map(String::trim).filter(s -> !s.isEmpty())
                        .toList();
                case "--tc" -> tc = v;
                case "--threads" -> threads = Integer.parseInt(v);
                case "--table-size" -> tableSize = Integer.parseInt(v);
                case "--branches" -> branches = Integer.parseInt(v);
                case "--reads" -> reads = Integer.parseInt(v);
                case "--writes" -> writes = Integer.parseInt(v);
                case "--skew" -> skewness = Double.parseDouble(v);
                case "--rmw" -> rmw = Boolean.parseBoolean(v);
                case "--warmup-s" -> warmup = seconds(v);
                case "--measure-s" -> measure = seconds(v);
                case "--txn-timeout-ms" -> txnTimeout = Duration.ofMillis(Long.parseLong(v));
                case "--parallel-branches" -> parallelBranches = Boolean.parseBoolean(v);
                case "--sonata" -> sonata = Boolean.parseBoolean(v);
                default -> throw new IllegalArgumentException("unknown option " + flag);
            }
        }
        return new BenchConfig(variant, shims, tc, threads, tableSize, branches, reads, writes, skewness, rmw, warmup,
                measure, txnTimeout, parallelBranches, sonata).validate();
    }

    private static Duration seconds(String v) {
        return Duration.ofMillis(Math.round(Double.parseDouble(v) * 1000));
    }
}
