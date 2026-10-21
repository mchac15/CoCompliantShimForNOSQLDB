package ch.epfl.coshim.bench;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLTransactionRollbackException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;
import javax.transaction.xa.XAException;
import org.apache.seata.common.ConfigurationKeys;
import org.apache.seata.core.context.RootContext;
import org.apache.seata.core.exception.TransactionException;
import org.apache.seata.core.exception.TransactionExceptionCode;
import org.apache.seata.core.model.GlobalStatus;
import org.apache.seata.rm.RMClient;
import org.apache.seata.tm.TMClient;
import org.apache.seata.tm.api.GlobalTransaction;
import org.apache.seata.tm.api.GlobalTransactionContext;

/**
 * Micro benchmark of the shim through the real path: a Seata TC decides every global transaction,
 * and the shims run in shim nodes ({@code CoShimNode}) reached over TCP. This JVM is the
 * application, like one of Acta's services: a Seata TM (begin / commit / rollback) and RM (one
 * Seata XA data source per participant).
 *
 * <p>A participant is a shim database ({@link CoShimParticipant}) or, to compare with Sonata, a
 * MySQL/PostgreSQL database behind Seata's stock XA proxy ({@link SqlParticipant}, Sonata on with
 * {@code sonata}). Everything else, the TC included, is the same for both, so the runs differ only
 * in what is behind the {@code XAResource}.
 *
 * <p>Workload: Acta's Micro (scripts in ../acta-server). Each worker runs global transactions in a
 * closed loop. A global transaction has {@code branches} branches; branch i reads {@code reads}
 * keys then writes {@code writes} other keys of table {@code micro-i}, on one of the participants
 * chosen at random. Hot and cold keys follow Acta's skew rule.
 *
 * <p>One attempt:
 *
 * <ol>
 *   <li>TM begin with {@code txnTimeout}: the TC creates the XID and starts its timeout;
 *   <li>per branch: an XA connection to its shim; {@code setAutoCommit(false)} registers the branch
 *       with the TC and starts it on the shim, the gets/puts go to the shim, {@code commit()} ends
 *       and prepares it (the shim votes). Branches run in order, or at the same time with
 *       {@code parallelBranches} (each thread binds the XID);
 *   <li>all YES: TM commit, the TC sends phase 2 to the RM, which commits each branch on its shim;
 *       any failure: TM rollback, the TC rolls every branch back on its shim, and the attempt is
 *       retried with the same plan, as Acta does. If the TC's timeout expires first, the TC rolls
 *       the branches back itself (the shims receive abort over their connections).
 * </ol>
 *
 * <p>With {@code rmw}, every write is {@code v := get(k) + 1} on keys that start at 0, so after
 * the run the sum of all values, read back from the participants, must equal committed txns ×
 * branches × writes (lost updates, partial commits). Attempts whose outcome is unknown (the TM got
 * no clear answer to its commit) widen the accepted range.
 *
 * <p>Failed branches in the measurement window are counted by cause (deadlock, lock timeout,
 * serialization failure, XA rollback / NO vote, ...); the shims' own counters (commits, aborts by
 * cause) are logged by the nodes.
 */
public final class MicroBench implements AutoCloseable {

    static final String APPLICATION_ID = "coshim-bench";
    static final String TX_SERVICE_GROUP = "default_tx_group";

    /** One branch of a plan: which shim, which table, which keys. */
    record Branch(int shim, String table, List<Integer> readKeys, List<Integer> writeKeys) {}

    enum Attempt { COMMITTED, ABORTED, TIMED_OUT, UNKNOWN }

    private static String seataTc;

    private final BenchConfig config;
    private final List<Participant> participants = new ArrayList<>();
    /** --parallel-branches only: runs the branches of each attempt; grows to threads × branches. */
    private final ExecutorService branchPool = Executors.newCachedThreadPool(r -> {
        Thread t = new Thread(r, "bench-branch");
        t.setDaemon(true);
        return t;
    });
    private final LongAdder committedTotal = new LongAdder();
    private final LongAdder unknownTotal = new LongAdder();
    private final AtomicBoolean stop = new AtomicBoolean();
    /** Failed branches in the measurement window, by cause. */
    private final Map<String, LongAdder> branchFailures = new ConcurrentHashMap<>();
    private volatile long measureStart = Long.MAX_VALUE;
    private volatile long measureEnd = Long.MIN_VALUE;

    /**
     * Connects to the TC (once per JVM) and to every participant, and sets their tables up (SQL
     * databases: micro tables reset to 0, plus Sonata's dummy table when Sonata is on).
     *
     * @param token the shim nodes' shared secret (COSHIM_TOKEN); only needed for shim databases
     */
    public MicroBench(BenchConfig config, String token) {
        this.config = config.validate();
        // before Seata's XA proxy class is loaded: it reads the switch once, into a static final
        System.setProperty(ConfigurationKeys.SONATA_ENABLE_GLOBAL_SERIALIZABILITY, String.valueOf(config.sonata()));
        initSeata(config.tc());
        try {
            for (String spec : config.shims()) {
                Participant p = Participant.connect(spec, token, config);
                participants.add(p);
                p.setUp(config.branches(), config.tableSize());
            }
        } catch (SQLException | RuntimeException e) {
            close();
            throw e instanceof RuntimeException r ? r : new IllegalStateException("setting the databases up failed", e);
        }
    }

    /** Seata's TM and RM clients are per JVM: connect them once, to one TC. */
    static synchronized void initSeata(String tc) {
        if (seataTc != null) {
            if (!seataTc.equals(tc)) {
                throw new IllegalStateException("this JVM is already connected to the TC at " + seataTc);
            }
            return;
        }
        System.setProperty("service.default.grouplist", tc);
        TMClient.init(APPLICATION_ID, TX_SERVICE_GROUP);
        RMClient.init(APPLICATION_ID, TX_SERVICE_GROUP);
        seataTc = tc;
    }

    public BenchResult run() throws InterruptedException {
        long start = System.nanoTime();
        long measureStart = start + config.warmup().toNanos();
        long measureEnd = measureStart + config.measure().toNanos();
        this.measureStart = measureStart;
        this.measureEnd = measureEnd;

        List<Worker> workers = new ArrayList<>();
        List<Thread> threads = new ArrayList<>();
        for (int i = 0; i < config.threads(); i++) {
            Worker w = new Worker(measureStart, measureEnd);
            workers.add(w);
            Thread t = new Thread(w, "bench-worker-" + i);
            threads.add(t);
            t.start();
        }

        // drain: in-flight attempts finish (bounded by the TC's timeout), then the workers exit
        long drainDeadline = measureEnd + config.txnTimeout().toNanos() + TimeUnit.SECONDS.toNanos(10);
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
        for (Worker w : workers) {
            if (w.error != null) {
                warning = "worker failed: " + w.error;
                break;
            }
        }

        long committed = 0;
        long aborted = 0;
        long timedOut = 0;
        long unknown = 0;
        List<Long> latencies = new ArrayList<>();
        for (Worker w : workers) {
            committed += w.committed;
            aborted += w.aborted;
            timedOut += w.timedOut;
            unknown += w.unknown;
            latencies.addAll(w.latenciesUs);
        }
        latencies.sort(null);

        long expectedMin = -1;
        long expectedMax = -1;
        long actual = -1;
        if (config.rmw() && stuck == 0 && warning == null) {
            long perTxn = (long) config.branches() * config.writes();
            expectedMin = committedTotal.sum() * perTxn;
            expectedMax = (committedTotal.sum() + unknownTotal.sum()) * perTxn;
            actual = storedSum(expectedMin);
        }
        Map<String, Long> failures = new TreeMap<>();
        branchFailures.forEach((cause, n) -> failures.put(cause, n.sum()));
        return new BenchResult(config, committed, aborted, timedOut, unknown, latencies, expectedMin, expectedMax,
                actual, failures, warning);
    }

    /**
     * The sum of every value of every micro table on every participant (local transactions, outside
     * any global one). Read again a few times while it is below {@code expectedMin}, in case the TC
     * still delivers phase-2 commits.
     */
    private long storedSum(long expectedMin) throws InterruptedException {
        long sum = 0;
        SQLTransactionRollbackException unsettled = null;
        for (int round = 0; round < 10; round++) {
            sum = 0;
            unsettled = null;
            for (Participant p : participants) {
                try {
                    sum += p.sum(config.branches(), config.tableSize());
                } catch (SQLTransactionRollbackException e) {
                    // the audit's reads take locks too: behind a branch the TC has not resolved yet,
                    // one can time out or be aborted with it. Not settled yet: read again
                    unsettled = e;
                    break;
                } catch (SQLException e) {
                    throw new IllegalStateException("audit read failed on " + p.spec(), e);
                }
            }
            if (unsettled == null && sum >= expectedMin) {
                break;
            }
            Thread.sleep(500);
        }
        if (unsettled != null) {
            throw new IllegalStateException("audit reads kept aborting for 10 rounds", unsettled);
        }
        return sum;
    }

    static String table(int branch) {
        return "micro-" + branch;
    }

    private List<Branch> nextPlan() {
        return randomPlan(config, participants.size());
    }

    /**
     * A new plan, as Acta's MicroWorkloadService: branch i on table micro-i, on a shim chosen at
     * random, with distinct keys across the whole global txn.
     */
    static List<Branch> randomPlan(BenchConfig config, int shims) {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        Set<Integer> used = new HashSet<>();
        List<Branch> plan = new ArrayList<>();
        for (int b = 0; b < config.branches(); b++) {
            List<Integer> reads = workingSet(config, config.reads(), used);
            List<Integer> writes = workingSet(config, config.writes(), used);
            plan.add(new Branch(random.nextInt(shims), table(b), reads, writes));
        }
        return plan;
    }

    /** Acta's makeWorkingSet: ops × skewness keys from the hot range, the rest from the cold range. */
    private static List<Integer> workingSet(BenchConfig config, int ops, Set<Integer> used) {
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

    /** One global transaction attempt: TM begin, phase 1 of every branch, TM commit or rollback. */
    Attempt attempt(List<Branch> plan) {
        GlobalTransaction tx = GlobalTransactionContext.createNew();
        try {
            tx.begin((int) config.txnTimeout().toMillis(), "micro");
        } catch (TransactionException e) {
            throw new IllegalStateException("TC at " + config.tc() + ": begin failed", e);
        }
        try {
            boolean allYes = config.parallelBranches() ? phaseOneInParallel(tx, plan) : phaseOneInOrder(tx, plan);
            if (!allYes) {
                return rollback(tx);
            }
            try {
                tx.commit();
            } catch (TransactionException e) {
                return e.getCode() == TransactionExceptionCode.TransactionTimeout ? Attempt.TIMED_OUT : Attempt.UNKNOWN;
            }
            GlobalStatus status = tx.getLocalStatus();
            if (status == GlobalStatus.Committed || status == GlobalStatus.AsyncCommitting) {
                return Attempt.COMMITTED;
            }
            if (isTimeout(status)) {
                return Attempt.TIMED_OUT;
            }
            return status == GlobalStatus.Rollbacked ? Attempt.ABORTED : Attempt.UNKNOWN;
        } finally {
            RootContext.unbind();
        }
    }

    private static boolean isTimeout(GlobalStatus status) {
        return status == GlobalStatus.TimeoutRollbacking || status == GlobalStatus.TimeoutRollbacked
                || status == GlobalStatus.TimeoutRollbackRetrying || status == GlobalStatus.TimeoutRollbackFailed;
    }

    /** TM rollback: the TC rolls every registered branch back on its shim. Nothing was committed. */
    private Attempt rollback(GlobalTransaction tx) {
        try {
            tx.rollback();
        } catch (TransactionException e) {
            // the TC refused (e.g. it already timed the txn out); it never commits it either way
            return e.getCode() == TransactionExceptionCode.TransactionTimeout ? Attempt.TIMED_OUT : Attempt.ABORTED;
        }
        return isTimeout(tx.getLocalStatus()) ? Attempt.TIMED_OUT : Attempt.ABORTED;
    }

    private boolean phaseOneInOrder(GlobalTransaction tx, List<Branch> plan) {
        for (Branch b : plan) {
            if (!runBranch(tx.getXid(), b, false)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Phase 1 of every branch at the same time. Returns only when every branch is done, so no branch
     * outlives its attempt; the caller then rolls back if any branch failed, which the TC turns into
     * an abort on every shim (including one where a sibling still waits in prepare: it votes NO).
     */
    private boolean phaseOneInParallel(GlobalTransaction tx, List<Branch> plan) {
        ExecutorCompletionService<Boolean> done = new ExecutorCompletionService<>(branchPool);
        for (Branch b : plan) {
            done.submit(() -> runBranch(tx.getXid(), b, true));
        }
        boolean allYes = true;
        RuntimeException error = null;
        for (int i = 0; i < plan.size(); i++) {
            try {
                allYes &= done.take().get();
            } catch (ExecutionException e) {
                allYes = false;
                if (error == null) {
                    error = e.getCause() instanceof RuntimeException r ? r : new IllegalStateException(e.getCause());
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                allYes = false;
            }
        }
        if (error != null) {
            throw error;
        }
        return allYes;
    }

    /**
     * Phase 1 of one branch over an XA connection to its participant: register + start, the
     * reads/writes, end + prepare (with Sonata, its dummy write comes right before prepare).
     *
     * @param bindXid true when running on a pool thread, which must join the global transaction
     * @return true iff the branch voted YES; false if the participant aborted it, voted NO, or the
     *     TC refused the branch (e.g. the txn already timed out)
     */
    private boolean runBranch(String xid, Branch b, boolean bindXid) {
        if (bindXid) {
            RootContext.bind(xid);
        }
        Participant p = participants.get(b.shim());
        try (Connection c = p.dataSource().getConnection()) {
            c.setAutoCommit(false);   // branchRegister with the TC + xa start on the participant
            try {
                for (int k : b.readKeys()) {
                    p.get(c, b.table(), k);
                }
                for (int k : b.writeKeys()) {
                    long value = config.rmw() ? p.get(c, b.table(), k) + 1
                            : ThreadLocalRandom.current().nextLong(1, Long.MAX_VALUE);
                    p.put(c, b.table(), k, value);
                }
                c.commit();   // xa end + xa prepare: the participant votes
                return true;
            } catch (SQLException e) {
                countFailure(e);
                try {
                    c.rollback();
                } catch (SQLException ignored) {
                    // the global rollback that follows aborts the branch anyway
                }
                return false;
            }
        } catch (SQLException e) {
            countFailure(e);
            return false;   // the branch could not be registered, started or prepared
        } finally {
            if (bindXid) {
                RootContext.unbind();
            }
        }
    }

    private void countFailure(SQLException e) {
        long now = System.nanoTime();
        if (now >= measureStart && now < measureEnd) {
            branchFailures.computeIfAbsent(failureCause(e), c -> new LongAdder()).increment();
        }
    }

    /**
     * Why a branch failed, from the first recognisable exception in the cause chain: MySQL
     * (deadlock 1213, lock wait timeout 1205), PostgreSQL (40P01, 55P03, serialization failure 40001,
     * also raised by Sonata's SSI dummy write), XA rollback codes (the shim's NO vote), the shim's
     * aborts during execution, and the TC refusing the branch.
     */
    static String failureCause(Throwable e) {
        for (Throwable t = e; t != null; t = t.getCause()) {
            if (t instanceof SQLException s) {
                if (s.getErrorCode() == 1213 || "40P01".equals(s.getSQLState())) {
                    return "deadlock";
                }
                if (s.getErrorCode() == 1205 || "55P03".equals(s.getSQLState())) {
                    return "lock_timeout";
                }
                if ("40001".equals(s.getSQLState())) {
                    return "serialization";
                }
                if (s instanceof SQLTransactionRollbackException && s.getSQLState() == null) {
                    return "shim_abort";   // the shim aborted the branch while it executed
                }
            }
            if (t instanceof TransactionException) {
                return "tc_refused";   // branch register / report refused, e.g. the txn already timed out
            }
            if (t instanceof XAException x) {
                if (x.errorCode == XAException.XA_RBDEADLOCK) {
                    return "deadlock";
                }
                if (x.errorCode == XAException.XA_RBTIMEOUT) {
                    return "lock_timeout";
                }
                if (x.errorCode >= XAException.XA_RBBASE && x.errorCode <= XAException.XA_RBEND) {
                    return "xa_rollback";
                }
                if (x.getMessage() != null && x.getMessage().contains("serialization failure")) {
                    return "serialization";
                }
            }
        }
        Throwable root = e;
        while (root.getCause() != null && root.getCause() != root) {
            root = root.getCause();
        }
        return "other:" + root.getClass().getSimpleName();
    }

    private final class Worker implements Runnable {
        final long measureStart;
        final long measureEnd;
        long committed;
        long aborted;
        long timedOut;
        long unknown;
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
                    case UNKNOWN -> {
                        // may or may not have committed: never retry it, and widen the audit's range
                        unknownTotal.increment();
                        unknown += measured ? 1 : 0;
                        plan = nextPlan();
                    }
                }
            }
        }
    }

    @Override
    public void close() {
        branchPool.shutdownNow();
        participants.forEach(Participant::close);
    }

    /**
     * {@code COSHIM_TOKEN=... java ... MicroBench [--variant LABEL] [--shims PARTICIPANT,...]
     * [--tc host:port] [--threads N] [--table-size N] [--branches N] [--reads N] [--writes N] [--skew X]
     * [--rmw true|false] [--warmup-s S] [--measure-s S] [--txn-timeout-ms MS]
     * [--parallel-branches true|false] [--sonata true|false] [--csv FILE] [--label TEXT]}, where a
     * participant is a shim database {@code host:port/db} or {@code jdbc:mysql://...} /
     * {@code jdbc:postgresql://...}. Needs a running Seata TC and the shim nodes ({@code CoShimNode})
     * or databases; COSHIM_TOKEN only with shim databases. The run is appended to {@code FILE} (default
     * {@code bench-results/<timestamp>-<variant>.csv}). Exit code 0 iff the run had no warning and the
     * audit passed.
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
        String token = System.getenv("COSHIM_TOKEN");
        if ((token == null || token.isEmpty()) && config.shims().stream().anyMatch(Participant::isCoShim)) {
            System.err.println("error: set COSHIM_TOKEN to the shim nodes' token");
            System.exit(2);
            return;
        }
        String csv = option(args, "--csv");
        if (csv == null) {
            csv = "bench-results/" + LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss"))
                    + "-" + config.variant() + ".csv";
        }
        String label = option(args, "--label");

        BenchResult result;
        try (MicroBench bench = new MicroBench(config, token)) {
            result = bench.run();
        }
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
