package ch.epfl.coshim.bench;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

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
 * @param reads            pure reads per branch
 * @param writes           writes per branch; a branch runs its reads and writes in a random order
 * @param skewness         share of the accesses that go to the hot range, the first (1 - skewness) * tableSize
 *                         keys, in [0.5, 1): 0.9 = 90 % of the accesses on 10 % of the keys (Acta's definition)
 * @param dist             how the keys of a branch are drawn: {@code hotspot}, each one independently from the
 *                         hot range with probability {@code skewness}, else from the cold range, uniformly
 *                         within each (the definition's intent: the share of hot accesses is the skewness,
 *                         whatever the op count); {@code acta}, Acta's makeWorkingSet, the first
 *                         ceil(ops * skewness) keys of each list hot (with a few ops per list every key is hot
 *                         from skewness 0.67 on: the skewness only sets the size of the hot range)
 * @param rmw              writes are v := get(k) + 1 instead of blind random values; enables the audit
 * @param warmup           run time before measuring
 * @param measure          measured run time
 * @param txnTimeout       global transaction timeout, enforced by the TC (rolls the branches back)
 * @param parallelBranches run the branches of a global transaction at the same time (each still
 *                         start, ops, end, prepare) instead of one after the other
 * @param sonata           turn Sonata on for the SQL databases ({@code sonata.enableGlobalSerializability};
 *                         Sonata's other settings stay at their defaults). Seata reads it once per JVM
 * @param opsMean          > 0: every branch draws its reads and its writes from a normal distribution of
 *                         this mean truncated to [0, opsMax] (not both 0), instead of {@code reads} /
 *                         {@code writes}; <= 0: the fixed counts
 * @param opsSd            standard deviation of that distribution
 * @param opsMax           largest number of reads (or writes) a branch draws
 * @param thinkMeanUs      > 0: the application pauses for a normal draw of this mean (µs, truncated at
 *                         0) before every operation of a branch but the first and before its commit,
 *                         standing for the network hop and the work between two requests; 0: no pause
 * @param thinkSdUs        standard deviation of that pause (µs)
 * @param sampleMs         > 0: every that many ms, the commits and aborts of the whole run (warm-up and drain
 *                         included) are sampled into {@code timeseries-<csv name>}; 0: no samples
 * @param randomness       share of a branch's operations issued in a random order, in [0, 1]: the first
 *                         round((1 - randomness) x ops) of them (a random subset of its keys) go in ascending
 *                         key order, the rest after them in a random order. 1 (default): every op in a random
 *                         order; 0: the whole branch locks its keys in ascending order. The keys are the same
 * @param getForUpdate     before a branch runs, its planned ops are scanned and every read of a key the branch
 *                         writes at that op or later (a read-modify-write's own read, or a pure read followed by a
 *                         write of its key) becomes a get-for-update: the shims lock the key EXCLUSIVE right away
 *                         instead of SHARED, then upgrading at the write. Other reads stay plain gets; SQL
 *                         participants still run a plain select. false (default): every read is a plain get
 */
public record BenchConfig(String variant, List<String> shims, String tc, int threads, int tableSize, int branches,
        int reads, int writes, double skewness, boolean rmw, Duration warmup, Duration measure, Duration txnTimeout,
        boolean parallelBranches, boolean sonata, double opsMean, double opsSd, int opsMax, double thinkMeanUs,
        double thinkSdUs, int sampleMs, KeyDist dist, double randomness, boolean getForUpdate) {

    /** How the keys of a branch are drawn; see {@code dist}. */
    public enum KeyDist {
        HOTSPOT, ACTA;

        static KeyDist parse(String v) {
            for (KeyDist d : values()) {
                if (d.name().equalsIgnoreCase(v)) {
                    return d;
                }
            }
            throw new IllegalArgumentException("--dist is hotspot or acta: " + v);
        }

        @Override
        public String toString() {
            return name().toLowerCase();
        }
    }

    public static BenchConfig defaults() {
        return new BenchConfig("unknown", List.of("127.0.0.1:7000/a", "127.0.0.1:7001/b"), "127.0.0.1:8091", 50,
                10_000, 2, 2, 2, 0.9, true, Duration.ofSeconds(10), Duration.ofSeconds(30), Duration.ofSeconds(10),
                false, false, 0, 1.5, 10, 0, 1000, 1000, KeyDist.HOTSPOT, 1.0, false);
    }

    /** Reads of one branch: {@code reads}, or a draw from the distribution. */
    public int drawReads() {
        return opsMean > 0 ? drawOps() : reads;
    }

    /** Writes of one branch: {@code writes}, or a draw from the distribution. */
    public int drawWrites() {
        return opsMean > 0 ? drawOps() : writes;
    }

    /** round(N(opsMean, opsSd)), drawn again until it is in [0, opsMax] (a truncated normal). */
    private int drawOps() {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        while (true) {
            long v = Math.round(random.nextGaussian(opsMean, opsSd));
            if (v >= 0 && v <= opsMax) {
                return (int) v;
            }
        }
    }

    /** One think time in ns: N(thinkMeanUs, thinkSdUs) truncated at 0; 0 when off. */
    public long drawThinkNanos() {
        if (thinkMeanUs <= 0) {
            return 0;
        }
        return Math.max(0, Math.round(ThreadLocalRandom.current().nextGaussian(thinkMeanUs, thinkSdUs) * 1000));
    }

    /** The most reads (writes) a branch can have. */
    private int maxReads() {
        return opsMean > 0 ? opsMax : reads;
    }

    private int maxWrites() {
        return opsMean > 0 ? opsMax : writes;
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
        require(opsMean <= 0 || (opsSd > 0 && opsMax >= 1), "--ops-sd must be > 0 and --ops-max >= 1");
        require(thinkMeanUs <= 0 || thinkSdUs >= 0, "--think-sd-us must be >= 0");
        require(sampleMs >= 0, "--sample-ms must be >= 0");
        require(randomness >= 0 && randomness <= 1, "--randomness must be in [0, 1]");
        // a txn's keys are distinct: the ranges must hold the most a txn can draw from each of them
        long hotPerBranch = dist == KeyDist.ACTA
                ? (long) Math.ceil(maxReads() * skewness) + (long) Math.ceil(maxWrites() * skewness)
                : (long) maxReads() + maxWrites();   // hotspot: every key of a branch may be drawn hot
        long coldPerBranch = dist == KeyDist.ACTA ? (long) maxReads() + maxWrites() - hotPerBranch
                : (long) maxReads() + maxWrites();   // ... or cold
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
        double opsMean = c.opsMean;
        double opsSd = c.opsSd;
        int opsMax = c.opsMax;
        double thinkMeanUs = c.thinkMeanUs;
        double thinkSdUs = c.thinkSdUs;
        int sampleMs = c.sampleMs;
        KeyDist dist = c.dist;
        double randomness = c.randomness;
        boolean getForUpdate = c.getForUpdate;
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
                case "--ops-mean" -> opsMean = Double.parseDouble(v);
                case "--ops-sd" -> opsSd = Double.parseDouble(v);
                case "--ops-max" -> opsMax = Integer.parseInt(v);
                case "--think-mean-us" -> thinkMeanUs = Double.parseDouble(v);
                case "--think-sd-us" -> thinkSdUs = Double.parseDouble(v);
                case "--sample-ms" -> sampleMs = Integer.parseInt(v);
                case "--dist" -> dist = KeyDist.parse(v);
                case "--randomness" -> randomness = Double.parseDouble(v);
                case "--get-for-update" -> getForUpdate = Boolean.parseBoolean(v);
                default -> throw new IllegalArgumentException("unknown option " + flag);
            }
        }
        return new BenchConfig(variant, shims, tc, threads, tableSize, branches, reads, writes, skewness, rmw, warmup,
                measure, txnTimeout, parallelBranches, sonata, opsMean, opsSd, opsMax, thinkMeanUs, thinkSdUs, sampleMs,
                dist, randomness, getForUpdate)
                .validate();
    }

    private static Duration seconds(String v) {
        return Duration.ofMillis(Math.round(Double.parseDouble(v) * 1000));
    }
}
