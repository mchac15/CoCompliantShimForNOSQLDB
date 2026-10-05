package ch.epfl.coshim.bench;

import ch.epfl.coshim.core.CoShim;
import ch.epfl.coshim.core.Outcome;
import ch.epfl.coshim.core.ShimStats;
import ch.epfl.coshim.core.SpeculativeCoShim;
import ch.epfl.coshim.core.TxnAbortedException;
import ch.epfl.coshim.core.Vote;
import ch.epfl.coshim.store.InMemoryKvStore;
import ch.epfl.coshim.store.TableKey;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.LockSupport;

/**
 * Standalone Micro benchmark of the shim: Acta's Micro workload (scripts in ../acta-server), run in
 * one JVM against two {@link SpeculativeCoShim}s ("a" and "b", one in-memory store each), with this
 * class playing the application and the 2PC coordinator. Nothing else is in the loop (no Seata, no
 * network), so a speculative run and a non-speculative run differ only in the speculation point.
 *
 * <p>Each worker runs global transactions in a closed loop. A global transaction has
 * {@code branches} branches; branch i reads {@code reads} keys then writes {@code writes} other keys
 * of table {@code micro-i}, on shim "a" or "b" at random. The coordinator follows Seata's XA mode:
 *
 * <ol>
 *   <li>per branch, in order: start, the gets/puts, end, then prepare right away (Seata's phase 1
 *       runs xa end + xa prepare when the branch's connection commits);
 *   <li>after the last YES vote, {@code commitDelay} (the TM → TC → RM round trip), then commit on
 *       every branch;
 *   <li>any abort or NO vote rolls every registered branch back; the attempt is retried with the
 *       same plan, as Acta does.
 * </ol>
 *
 * With {@code parallelBranches}, step 1 runs every branch at the same time instead (each still
 * start, ops, end, prepare), and the first failure rolls the whole transaction back. Branches then
 * no longer take the shims in the same order in every transaction, so two transactions can wait on
 * each other's commit in prepare on different shims (a cross-shim cycle); only the global timeout
 * below breaks it, and those attempts are counted as timed out.
 *
 * <p>A global transaction timeout plays the TC's: it rolls a stuck attempt back (e.g. a prepare that
 * waits forever on a cross-shim cycle). Commit and timeout race on one state CAS, so a global
 * transaction is either committed on every branch or rolled back on every branch.
 *
 * <p>With {@code rmw}, every write is {@code v := get(k) + 1} on keys that start at 0, so after the
 * run the sum of all values must equal committed txns × branches × writes. Any lost update, partial
 * commit or commit of an aborted txn shows up there.
 */
public final class MicroBench {

    private static final int ACTIVE = 0;
    private static final int COMMITTING = 1;
    private static final int ROLLED_BACK = 2;

    /** One branch of a plan: which shim, which table, which keys. */
    record Branch(int shim, String table, List<Integer> readKeys, List<Integer> writeKeys) {}

    private enum Attempt { COMMITTED, ABORTED, TIMED_OUT }

    private final BenchConfig config;
    private final List<InMemoryKvStore<TableKey<Integer>, Integer>> stores = new ArrayList<>();
    private final List<SpeculativeCoShim<TableKey<Integer>, Integer>> shims = new ArrayList<>();
    private final ScheduledThreadPoolExecutor timeouts = new ScheduledThreadPoolExecutor(1, r -> {
        Thread t = new Thread(r, "bench-txn-timeout");
        t.setDaemon(true);
        return t;
    });
    /** --parallel-branches only: runs the branches of each attempt; grows to threads × branches. */
    private final ExecutorService branchPool = Executors.newCachedThreadPool(r -> {
        Thread t = new Thread(r, "bench-branch");
        t.setDaemon(true);
        return t;
    });
    private final AtomicLong globalIds = new AtomicLong();
    private final LongAdder committedTotal = new LongAdder();
    private final AtomicBoolean stop = new AtomicBoolean();

    public MicroBench(BenchConfig config) {
        this.config = config.validate();
        for (int s = 0; s < 2; s++) {
            InMemoryKvStore<TableKey<Integer>, Integer> store = new InMemoryKvStore<>();
            stores.add(store);
            shims.add(new SpeculativeCoShim<>(store, config.lockTimeout(), SpeculativeCoShim.DEFAULT_TOMBSTONE_TTL,
                    config.speculative()));
        }
        timeouts.setRemoveOnCancelPolicy(true);
    }

    public BenchResult run() throws InterruptedException {
        long start = System.nanoTime();
        long measureStart = start + config.warmup().toNanos();
        long measureEnd = measureStart + config.measure().toNanos();

        List<Worker> workers = new ArrayList<>();
        List<Thread> threads = new ArrayList<>();
        for (int i = 0; i < config.threads(); i++) {
            Worker w = new Worker(measureStart, measureEnd);
            workers.add(w);
            Thread t = new Thread(w, "bench-worker-" + i);
            threads.add(t);
            t.start();
        }

        // drain: in-flight attempts finish (bounded by the txn timeout), then the workers exit
        long drainDeadline = measureEnd + config.txnTimeout().toNanos() + config.commitDelay().toNanos()
                + TimeUnit.SECONDS.toNanos(5);
        for (Thread t : threads) {
            t.join(Math.max(1, TimeUnit.NANOSECONDS.toMillis(drainDeadline - System.nanoTime())));
        }
        String warning = null;
        long stuck = threads.stream().filter(Thread::isAlive).count();
        if (stuck > 0) {
            warning = stuck + " worker(s) still running after the drain deadline; audit skipped";
            stop.set(true);
            threads.forEach(Thread::interrupt);
            for (Thread t : threads) {
                t.join(1000);
            }
        }
        timeouts.shutdownNow();
        branchPool.shutdownNow();
        for (Worker w : workers) {
            if (w.error != null) {
                warning = "worker failed: " + w.error;
                break;
            }
        }

        long committed = 0;
        long aborted = 0;
        long timedOut = 0;
        List<Long> latencies = new ArrayList<>();
        for (Worker w : workers) {
            committed += w.committed;
            aborted += w.aborted;
            timedOut += w.timedOut;
            latencies.addAll(w.latenciesUs);
        }
        latencies.sort(null);

        long expected = -1;
        long actual = -1;
        if (config.rmw() && stuck == 0) {
            expected = committedTotal.sum() * config.branches() * config.writes();
            actual = storedSum();
        }
        List<ShimStats> stats = shims.stream().map(SpeculativeCoShim::stats).toList();
        return new BenchResult(config, committed, aborted, timedOut, latencies, stats, expected, actual, warning);
    }

    /** The sum of every value of every micro table, in both stores. */
    private long storedSum() {
        long sum = 0;
        for (InMemoryKvStore<TableKey<Integer>, Integer> store : stores) {
            for (int b = 0; b < config.branches(); b++) {
                for (int k = 0; k < config.tableSize(); k++) {
                    Integer v = store.get(new TableKey<>(table(b), k));
                    sum += v == null ? 0 : v;
                }
            }
        }
        return sum;
    }

    private static String table(int branch) {
        return "micro-" + branch;
    }

    /** A new plan, as Acta's MicroWorkloadService: distinct keys across the whole global txn. */
    List<Branch> nextPlan() {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        Set<Integer> used = new HashSet<>();
        List<Branch> plan = new ArrayList<>();
        for (int b = 0; b < config.branches(); b++) {
            List<Integer> reads = workingSet(config.reads(), used);
            List<Integer> writes = workingSet(config.writes(), used);
            int shim = random.nextInt(100) < config.shimBPercent() ? 1 : 0;
            plan.add(new Branch(shim, table(b), reads, writes));
        }
        return plan;
    }

    /** Acta's makeWorkingSet: ops × skewness keys from the hot range, the rest from the cold range. */
    private List<Integer> workingSet(int ops, Set<Integer> used) {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        int hot = config.hotRangeSize();
        List<Integer> keys = new ArrayList<>();
        while (keys.size() < ops * config.skewness()) {
            int candidate = random.nextInt(0, hot);
            if (used.add(candidate)) {
                keys.add(candidate);
            }
        }
        while (keys.size() < ops) {
            int candidate = random.nextInt(hot, config.tableSize());
            if (used.add(candidate)) {
                keys.add(candidate);
            }
        }
        return keys;
    }

    /** One global transaction attempt, as the coordinator sees it. */
    private final class GlobalTxn {
        final String id = "g" + globalIds.incrementAndGet();
        final AtomicInteger state = new AtomicInteger(ACTIVE);
        /** Registered branches, in order; read by the timeout thread. */
        final List<String> branchIds = new CopyOnWriteArrayList<>();
        final List<CoShim<TableKey<Integer>, Integer>> branchShims = new CopyOnWriteArrayList<>();

        /** Like Seata's branchRegister: before start, so a timeout can roll the branch back. */
        synchronized void register(CoShim<TableKey<Integer>, Integer> shim, String branchId) {
            branchShims.add(shim);
            branchIds.add(branchId);
        }

        /** The TC's global timeout: rolls back unless the coordinator already decided commit. */
        void timeout() {
            if (state.compareAndSet(ACTIVE, ROLLED_BACK)) {
                abortAll();
            }
        }

        /** The coordinator's rollback after a failed branch; TIMED_OUT if the timeout did it first. */
        Attempt rollback() {
            if (state.compareAndSet(ACTIVE, ROLLED_BACK)) {
                abortAll();
                return Attempt.ABORTED;
            }
            return Attempt.TIMED_OUT;
        }

        private synchronized void abortAll() {
            for (int i = 0; i < branchIds.size(); i++) {
                branchShims.get(i).abort(branchIds.get(i));
            }
        }
    }

    private Attempt attempt(List<Branch> plan) {
        GlobalTxn g = new GlobalTxn();
        ScheduledFuture<?> timer =
                timeouts.schedule(g::timeout, config.txnTimeout().toMillis(), TimeUnit.MILLISECONDS);
        try {
            if (config.parallelBranches()) {
                Attempt failed = phaseOneInParallel(g, plan);
                if (failed != null) {
                    return failed;
                }
            } else {
                for (int i = 0; i < plan.size(); i++) {
                    if (!runBranch(g, plan.get(i), i)) {
                        return g.rollback();
                    }
                }
            }
            if (!g.state.compareAndSet(ACTIVE, COMMITTING)) {
                return Attempt.TIMED_OUT;   // the timeout rolled every branch back already
            }
            if (!config.commitDelay().isZero()) {
                LockSupport.parkNanos(config.commitDelay().toNanos());
            }
            for (int i = 0; i < g.branchIds.size(); i++) {
                g.branchShims.get(i).commit(g.branchIds.get(i));
            }
            return Attempt.COMMITTED;
        } catch (TxnAbortedException e) {
            return g.rollback();
        } finally {
            timer.cancel(false);
        }
    }

    /**
     * Phase 1 of one branch: register (like Seata's branchRegister), start, the gets/puts, end, and
     * prepare right away.
     *
     * @return true iff the branch voted YES
     * @throws TxnAbortedException if the shim aborted the branch during a get/put
     */
    private boolean runBranch(GlobalTxn g, Branch b, int index) {
        CoShim<TableKey<Integer>, Integer> shim = shims.get(b.shim());
        String branchId = g.id + "-" + index;
        g.register(shim, branchId);
        if (g.state.get() != ACTIVE || shim.start(branchId) != Outcome.SUCCEEDED) {
            return false;
        }
        for (int k : b.readKeys()) {
            shim.get(branchId, new TableKey<>(b.table(), k));
        }
        for (int k : b.writeKeys()) {
            TableKey<Integer> key = new TableKey<>(b.table(), k);
            int value;
            if (config.rmw()) {
                Integer old = shim.get(branchId, key);
                value = (old == null ? 0 : old) + 1;
            } else {
                value = ThreadLocalRandom.current().nextInt(1, Integer.MAX_VALUE);
            }
            shim.put(branchId, key, value);
        }
        return shim.end(branchId) == Outcome.SUCCEEDED && shim.prepare(branchId) == Vote.YES;
    }

    /**
     * Phase 1 of every branch at the same time (--parallel-branches). On the first failure the
     * coordinator rolls the whole transaction back at once, which also releases a sibling branch
     * blocked in prepare. Returns only when every branch is done, so no branch outlives its attempt.
     *
     * @return null if every branch voted YES, else the attempt's outcome (rolled back)
     */
    private Attempt phaseOneInParallel(GlobalTxn g, List<Branch> plan) {
        ExecutorCompletionService<Boolean> done = new ExecutorCompletionService<>(branchPool);
        for (int i = 0; i < plan.size(); i++) {
            int index = i;
            done.submit(() -> runBranch(g, plan.get(index), index));
        }
        Attempt outcome = null;
        RuntimeException error = null;
        for (int i = 0; i < plan.size(); i++) {
            boolean yes;
            try {
                yes = done.take().get();
            } catch (ExecutionException e) {
                yes = false;
                if (!(e.getCause() instanceof TxnAbortedException) && error == null) {
                    error = e.getCause() instanceof RuntimeException r ? r : new IllegalStateException(e.getCause());
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                yes = false;
            }
            if (!yes && outcome == null) {
                outcome = g.rollback();
            }
        }
        if (error != null) {
            throw error;
        }
        return outcome;
    }

    private final class Worker implements Runnable {
        final long measureStart;
        final long measureEnd;
        long committed;
        long aborted;
        long timedOut;
        final List<Long> latenciesUs = new ArrayList<>();
        volatile Throwable error;

        Worker(long measureStart, long measureEnd) {
            this.measureStart = measureStart;
            this.measureEnd = measureEnd;
        }

        @Override
        public void run() {
            List<Branch> plan = nextPlan();
            while (!stop.get() && System.nanoTime() < measureEnd) {
                long t0 = System.nanoTime();
                Attempt result;
                try {
                    result = attempt(plan);
                } catch (RuntimeException e) {
                    error = e;
                    stop.set(true);
                    return;
                }
                long t1 = System.nanoTime();
                boolean measured = t1 >= measureStart && t1 < measureEnd;
                switch (result) {
                    case COMMITTED -> {
                        committedTotal.increment();
                        if (measured) {
                            committed++;
                            latenciesUs.add((t1 - t0) / 1000);
                        }
                        plan = nextPlan();
                    }
                    case ABORTED -> aborted += measured ? 1 : 0;
                    case TIMED_OUT -> timedOut += measured ? 1 : 0;
                }
            }
        }
    }

    /**
     * {@code java ... MicroBench [--shim speculative|nonspeculative] [--threads N] [--table-size N]
     * [--branches N] [--reads N] [--writes N] [--skew X] [--shim-b-percent P] [--rmw true|false]
     * [--warmup-s S] [--measure-s S] [--commit-delay-ms MS] [--lock-timeout-ms MS] [--txn-timeout-ms MS]
     * [--parallel-branches true|false]
     * [--csv FILE] [--label TEXT]}. The run is appended to {@code FILE} (default
     * {@code bench-results/<timestamp>-<shim>.csv} in the working directory). Exit code 0 iff the run had
     * no warning and the audit passed.
     */
    public static void main(String[] args) throws Exception {
        BenchConfig config;
        try {
            config = BenchConfig.parse(args);
        } catch (IllegalArgumentException e) {
            System.err.println("error: " + e.getMessage());
            System.exit(2);
            return;
        }
        String csv = option(args, "--csv");
        if (csv == null) {
            csv = "bench-results/" + LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss"))
                    + "-" + config.shim() + ".csv";
        }
        String label = option(args, "--label");

        BenchResult result = new MicroBench(config).run();
        System.out.println(result.summary());
        Path file = Path.of(csv).toAbsolutePath();
        appendCsv(file, result.csvRow(label == null ? "" : label));
        System.out.println("  results appended to " + file);
        System.exit(result.ok() ? 0 : 1);
    }

    private static String option(String[] args, String flag) {
        for (int i = 0; i + 1 < args.length; i++) {
            if (args[i].equals(flag)) {
                return args[i + 1];
            }
        }
        return null;
    }

    private static void appendCsv(Path file, String row) throws IOException {
        if (file.getParent() != null) {
            Files.createDirectories(file.getParent());
        }
        if (!Files.exists(file)) {
            Files.writeString(file, BenchResult.csvHeader() + System.lineSeparator());
        }
        Files.writeString(file, row + System.lineSeparator(), StandardOpenOption.APPEND);
    }
}
