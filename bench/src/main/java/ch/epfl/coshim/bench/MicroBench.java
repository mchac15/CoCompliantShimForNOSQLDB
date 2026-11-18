package ch.epfl.coshim.bench;

import ch.epfl.coshim.jdbc.CoShimDataSource;
import ch.epfl.coshim.jdbc.KvSession;
import ch.epfl.coshim.net.Codec;
import ch.epfl.coshim.net.RemoteCoShim;
import ch.epfl.coshim.net.TableKeyCodec;
import ch.epfl.coshim.seata.DataSourceProxyCoShim;
import ch.epfl.coshim.store.TableKey;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.LockSupport;
import javax.sql.DataSource;
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
 * {@link DataSourceProxyCoShim} per shim database).
 *
 * <p>Workload: Acta's Micro (scripts in ../acta-server). Each worker runs global transactions in a
 * closed loop. A global transaction has {@code branches} branches; branch i reads some keys and writes
 * other keys of table {@code micro-i}, in a random order, on one of the shims chosen at random. The
 * counts are fixed ({@code reads}, {@code writes}) or drawn per branch ({@code opsMean}); the keys are
 * hot or cold by the skewness ({@code dist}); the application pauses between two operations ({@code
 * thinkMeanUs}).
 *
 * <p>One attempt:
 *
 * <ol>
 *   <li>TM begin with {@code txnTimeout}: the TC creates the XID and starts its timeout;
 *   <li>per branch: an XA connection to its shim; {@code setAutoCommit(false)} registers the branch
 *       with the TC and starts it on the shim, the gets/puts go to the shim, closing the connection
 *       ends and prepares it (the shim votes). Branches run in order, each starting when the previous
 *       one finished its operations with {@code pipelinedBranches} (its vote then comes in the
 *       background), or at the same time with {@code parallelBranches} (each thread binds the XID);
 *   <li>all YES: TM commit, the TC sends phase 2 to the RM, which commits each branch on its shim;
 *       any failure: TM rollback, the TC rolls every branch back on its shim, and the attempt is
 *       retried with the same plan, as Acta does. If the TC's timeout expires first, the TC rolls
 *       the branches back itself (the shims receive abort over their connections).
 * </ol>
 *
 * <p>With {@code rmw}, every write is {@code v := get(k) + 1} on keys that start empty, so after
 * the run the sum of all values, read back through the shims, must equal the writes of the committed
 * txns (lost updates, partial commits). Attempts whose outcome is unknown (the TM got no clear
 * answer to its commit) widen the accepted range.
 *
 * <p>The shims' own counters (commits, aborts by cause) are logged by the nodes.
 */
public final class MicroBench implements AutoCloseable {

    static final String APPLICATION_ID = "coshim-bench";
    static final String TX_SERVICE_GROUP = "default_tx_group";

    /** One branch of a plan: which shim, which table, which keys, and its ops in the order it runs them. */
    record Branch(int shim, String table, List<Integer> readKeys, List<Integer> writeKeys, List<Op> ops) {}

    /** One operation of a branch: a read of {@code key}, or a write of it (read-modify-write with rmw). */
    record Op(int key, boolean write) {}

    enum Attempt { COMMITTED, ABORTED, TIMED_OUT, UNKNOWN }

    private static String seataTc;

    private final BenchConfig config;
    private final List<RemoteCoShim<TableKey<String>, String>> remotes = new ArrayList<>();
    private final List<DataSource> dataSources = new ArrayList<>();
    /** --parallel-branches: runs the branches of each attempt; --pipelined-branches: their votes. */
    private final ExecutorService branchPool = Executors.newCachedThreadPool(r -> {
        Thread t = new Thread(r, "bench-branch");
        t.setDaemon(true);
        return t;
    });
    /** Whole run: the writes of the committed txns, and of those whose outcome is unknown (the audit). */
    private final LongAdder committedWrites = new LongAdder();
    private final LongAdder unknownWrites = new LongAdder();
    private final AtomicBoolean stop = new AtomicBoolean();

    /**
     * Connects to the TC (once per JVM) and to every shim database.
     *
     * @param token the shim nodes' shared secret (COSHIM_TOKEN)
     */
    public MicroBench(BenchConfig config, String token) {
        this.config = config.validate();
        initSeata(config.tc());
        for (String shim : config.shims()) {
            int colon = shim.indexOf(':');
            int slash = shim.indexOf('/');
            InetSocketAddress node = new InetSocketAddress(shim.substring(0, colon),
                    Integer.parseInt(shim.substring(colon + 1, slash)));
            RemoteCoShim<TableKey<String>, String> remote = new RemoteCoShim<>(node, shim.substring(slash + 1), token,
                    new TableKeyCodec<>(Codec.UTF8), Codec.UTF8).verify();
            remotes.add(remote);
            dataSources.add(new DataSourceProxyCoShim(new CoShimDataSource<>(shim, remote)));
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
            expectedMin = committedWrites.sum();
            expectedMax = committedWrites.sum() + unknownWrites.sum();
            actual = storedSum(expectedMin);
        }
        return new BenchResult(config, committed, aborted, timedOut, unknown, latencies, expectedMin, expectedMax,
                actual, warning);
    }

    /**
     * The sum of every value of every micro table on every shim, read through the shims (local
     * transactions, outside any global one). Read again a few times while it is below
     * {@code expectedMin}, in case the TC still delivers phase-2 commits.
     */
    private long storedSum(long expectedMin) throws InterruptedException {
        long sum = 0;
        for (int round = 0; round < 10; round++) {
            sum = 0;
            for (DataSource ds : dataSources) {
                try (Connection c = ds.getConnection()) {
                    KvSession<String, String> kv = KvSession.from(c);
                    for (int b = 0; b < config.branches(); b++) {
                        for (int k = 0; k < config.tableSize(); k++) {
                            String v = kv.get(table(b), String.valueOf(k));
                            sum += v == null ? 0 : Long.parseLong(v);
                        }
                    }
                } catch (SQLException e) {
                    throw new IllegalStateException("audit read failed", e);
                }
            }
            if (sum >= expectedMin) {
                break;
            }
            Thread.sleep(500);
        }
        return sum;
    }

    private static String table(int branch) {
        return "micro-" + branch;
    }

    private List<Branch> nextPlan() {
        return randomPlan(config, dataSources.size());
    }

    /**
     * A new plan, as Acta's MicroWorkloadService: branch i on table micro-i, on a shim chosen at
     * random, with distinct keys across the whole global txn; each branch's reads and writes in a
     * random order.
     */
    static List<Branch> randomPlan(BenchConfig config, int shims) {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        Set<Integer> used = new HashSet<>();
        List<Branch> plan = new ArrayList<>();
        for (int b = 0; b < config.branches(); b++) {
            int r;
            int w;
            do {
                r = config.drawReads();
                w = config.drawWrites();
            } while (r + w == 0);
            List<Integer> reads = workingSet(config, r, used);
            List<Integer> writes = workingSet(config, w, used);
            List<Op> ops = new ArrayList<>();
            reads.forEach(k -> ops.add(new Op(k, false)));
            writes.forEach(k -> ops.add(new Op(k, true)));
            Collections.shuffle(ops, random);
            plan.add(new Branch(random.nextInt(shims), table(b), reads, writes, List.copyOf(ops)));
        }
        return plan;
    }

    /**
     * {@code ops} distinct keys (none in {@code used}, which receives them): hotspot, each from the hot
     * range with probability skewness, else from the cold range; acta, Acta's makeWorkingSet, ops ×
     * skewness keys from the hot range, the rest from the cold range.
     */
    private static List<Integer> workingSet(BenchConfig config, int ops, Set<Integer> used) {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        int hot = config.hotRangeSize();
        List<Integer> keys = new ArrayList<>();
        if (config.dist() == BenchConfig.KeyDist.HOTSPOT) {
            while (keys.size() < ops) {
                // validate() made both ranges large enough for a whole txn: the redraws end
                boolean isHot = random.nextDouble() < config.skewness();
                int candidate = isHot ? random.nextInt(0, hot) : random.nextInt(hot, config.tableSize());
                if (used.add(candidate)) {
                    keys.add(candidate);
                }
            }
            return keys;
        }
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
            boolean allYes = config.parallelBranches() ? phaseOneInParallel(tx, plan)
                    : config.pipelinedBranches() ? phaseOnePipelined(tx, plan)
                    : phaseOneInOrder(tx, plan);
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
     * Phase 1 with the branches pipelined: one after the other on this thread, but a branch's end +
     * prepare (Seata's close() of its XA connection; its commit() only checks the state) runs on a
     * pool thread, so the next branch starts as soon as this one finished its operations instead of
     * after its vote. Starts no further branch once one failed or a vote already came back NO; returns
     * when every started branch voted.
     */
    private boolean phaseOnePipelined(GlobalTransaction tx, List<Branch> plan) {
        String xid = tx.getXid();
        List<Future<Boolean>> votes = new ArrayList<>();
        boolean executed = true;
        for (Branch b : plan) {
            if (votes.stream().anyMatch(v -> v.isDone() && !voteOf(v))) {
                executed = false;   // a vote came back NO: the txn rolls back anyway
                break;
            }
            Connection c;
            try {
                c = dataSources.get(b.shim()).getConnection();
            } catch (SQLException e) {
                executed = false;
                break;
            }
            try {
                c.setAutoCommit(false);   // branchRegister with the TC + xa start on the shim
                executeBranch(c, b);
                c.commit();   // only checks the XA state: end + prepare happen in close()
            } catch (SQLException e) {
                try {
                    c.rollback();
                } catch (SQLException ignored) {
                    // the global rollback that follows aborts the branch anyway
                }
                try {
                    c.close();
                } catch (SQLException ignored) {
                    // the branch failed already
                }
                executed = false;
                break;
            }
            votes.add(branchPool.submit(() -> {
                RootContext.bind(xid);
                try {
                    c.close();   // xa end + xa prepare: the shim votes
                    return true;
                } catch (SQLException e) {
                    return false;
                } finally {
                    RootContext.unbind();
                }
            }));
        }
        boolean allYes = executed;
        for (Future<Boolean> v : votes) {
            allYes &= voteOf(v);
        }
        return allYes;
    }

    /** A pipelined branch's vote, waiting for it: false if its prepare failed or the wait was interrupted. */
    private static boolean voteOf(Future<Boolean> vote) {
        try {
            return vote.get();
        } catch (ExecutionException e) {
            return false;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    /**
     * Phase 1 of one branch over an XA connection to its shim: register + start, the gets/puts,
     * end + prepare.
     *
     * @param bindXid true when running on a pool thread, which must join the global transaction
     * @return true iff the branch voted YES; false if the shim aborted it, voted NO, or the TC
     *     refused the branch (e.g. the txn already timed out)
     */
    private boolean runBranch(String xid, Branch b, boolean bindXid) {
        if (bindXid) {
            RootContext.bind(xid);
        }
        try (Connection c = dataSources.get(b.shim()).getConnection()) {
            c.setAutoCommit(false);   // branchRegister with the TC + xa start on the shim
            try {
                executeBranch(c, b);
                c.commit();   // only checks the XA state: end + prepare (the vote) happen in close()
                return true;
            } catch (SQLException e) {
                try {
                    c.rollback();
                } catch (SQLException ignored) {
                    // the global rollback that follows aborts the branch anyway
                }
                return false;
            }
        } catch (SQLException e) {
            return false;   // the branch could not be registered or started
        } finally {
            if (bindXid) {
                RootContext.unbind();
            }
        }
    }

    /**
     * A branch's operations on its started XA connection, in the plan's order. With {@code
     * thinkMeanUs}, the application pauses before every operation but the first and before the
     * prepare, holding its locks meanwhile. With {@code getForUpdate}, the reads of keys the branch
     * writes at that op or later are gets-for-update.
     */
    private void executeBranch(Connection c, Branch b) throws SQLException {
        KvSession<String, String> kv = KvSession.from(c);
        boolean[] forUpdate = config.getForUpdate() ? readsForUpdate(b.ops(), config.rmw())
                : new boolean[b.ops().size()];
        for (int i = 0; i < b.ops().size(); i++) {
            Op op = b.ops().get(i);
            if (i > 0) {
                think();
            }
            String key = String.valueOf(op.key());
            if (!op.write()) {
                read(kv, b.table(), key, forUpdate[i]);
                continue;
            }
            long value;
            if (config.rmw()) {
                String old = read(kv, b.table(), key, forUpdate[i]);
                value = (old == null ? 0 : Long.parseLong(old)) + 1;
            } else {
                value = ThreadLocalRandom.current().nextLong(1, Long.MAX_VALUE);
            }
            kv.put(b.table(), key, String.valueOf(value));
        }
        think();
    }

    /** A read of the branch: a get-for-update if the plan marked it so, else a get. */
    private static String read(KvSession<String, String> kv, String table, String key, boolean forUpdate)
            throws SQLException {
        return forUpdate ? kv.getForUpdate(table, key) : kv.get(table, key);
    }

    /**
     * The planning pass of {@code getForUpdate}, before a branch runs: for every op, whether its read
     * (if it reads) must be a get-for-update, i.e. whether the branch writes that key at this op or
     * later. A read-modify-write's own read is one (its put follows); a pure read is one only if a
     * later op writes its key; a read after the branch's last write of a key is a plain get (the key
     * is already locked EXCLUSIVE then). A blind write (without {@code rmw}) reads nothing.
     */
    static boolean[] readsForUpdate(List<Op> ops, boolean rmw) {
        boolean[] forUpdate = new boolean[ops.size()];
        Set<Integer> writtenLater = new HashSet<>();
        for (int i = ops.size() - 1; i >= 0; i--) {
            Op op = ops.get(i);
            if (op.write()) {
                writtenLater.add(op.key());
                forUpdate[i] = rmw;   // the read before its own put
            } else {
                forUpdate[i] = writtenLater.contains(op.key());
            }
        }
        return forUpdate;
    }

    /** Pauses for one think time (none when {@code thinkMeanUs} is off). */
    private void think() {
        long nanos = config.drawThinkNanos();
        if (nanos > 0) {
            LockSupport.parkNanos(nanos);
        }
    }

    private static long writes(List<Branch> plan) {
        return plan.stream().mapToLong(b -> b.writeKeys().size()).sum();
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
                        committedWrites.add(writes(plan));
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
                        unknownWrites.add(writes(plan));
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
        remotes.forEach(RemoteCoShim::close);
    }

    /**
     * {@code COSHIM_TOKEN=... java ... MicroBench [--variant LABEL] [--shims host:port/db,...]
     * [--tc host:port] [--threads N] [--table-size N] [--branches N] [--reads N] [--writes N] [--skew X]
     * [--ops-mean X] [--ops-sd X] [--ops-max N] [--think-mean-us X] [--think-sd-us X] [--dist hotspot|acta]
     * [--rmw true|false] [--get-for-update true|false] [--warmup-s S] [--measure-s S] [--txn-timeout-ms MS]
     * [--parallel-branches true|false] [--pipelined-branches true|false] [--csv FILE] [--label TEXT]}
     * (see {@link BenchConfig} for each option and its default). Needs a running Seata TC and the
     * shim nodes ({@code CoShimNode}). The run is appended to {@code FILE} (default
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
        if (token == null || token.isEmpty()) {
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
