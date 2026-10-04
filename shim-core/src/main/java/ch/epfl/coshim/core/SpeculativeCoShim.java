package ch.epfl.coshim.core;

import ch.epfl.coshim.core.ShimStats.AbortCause;
import ch.epfl.coshim.store.KvStore;
import ch.epfl.coshim.types.LockType;
import ch.epfl.coshim.types.LocksMap;
import ch.epfl.coshim.types.LocksMap.LockResult;
import ch.epfl.coshim.types.Transaction;
import ch.epfl.coshim.types.Transaction.Status;
import java.time.Duration;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The speculative CO-compliant shim of pseudo.txt (S2PL + speculation at {@code executed}). The
 * lock table (chains, lock/upgrade, cascades) lives in {@link LocksMap}; this class keeps
 * transactions_map, the status CASes, the buffers and the tombstones.
 *
 * <p>With {@code speculative = false} it is the non-speculative baseline: plain strict 2PL, where
 * lock() waits until the predecessors committed instead of executed. Same code otherwise, so a
 * benchmark of the two isolates what speculation buys (shorter lock waits) and costs (cascades).
 */
public class SpeculativeCoShim<K, V> implements CoShim<K, V> {
  /** Above Seata's default 60 s global transaction timeout (pseudo.txt tombstone_ttl). */
  public static final Duration DEFAULT_TOMBSTONE_TTL = Duration.ofSeconds(180);

  private static final Duration GC_PERIOD = Duration.ofSeconds(1);
  /** The request came after end(): the write buffer is final. */
  private static final Set<Status> FINISHED =
      EnumSet.of(Status.EXECUTED, Status.PREPARED, Status.COMMITTING, Status.COMMITTED);
  /** Voted YES already. */
  private static final Set<Status> VOTED_YES =
      EnumSet.of(Status.PREPARED, Status.COMMITTING, Status.COMMITTED);
  private static final Set<Status> PREPARABLE = EnumSet.of(Status.EXECUTED, Status.PREPARED);
  private static final Set<Status> ABORTABLE =
      EnumSet.of(Status.STARTED, Status.EXECUTED, Status.MUST_ABORT, Status.PREPARED);

  private final KvStore<K, V> store;
  private final Duration lockTimeout;
  private final Duration tombstoneTtl;
  private final LocksMap<K, V> locksMap;
  private final ShimStats stats = new ShimStats();
  private final Map<String, Transaction<K, V>> transactionsMap = new ConcurrentHashMap<>();

  public SpeculativeCoShim(KvStore<K, V> store, Duration lockTimeout) {
    this(store, lockTimeout, DEFAULT_TOMBSTONE_TTL);
  }

  /**
   * @param lockTimeout bounds the waits of lock()/upgrade() (deadlock breaking)
   * @param tombstoneTtl how long an abort-before-start is remembered; at least the coordinator's
   *     global transaction timeout
   */
  public SpeculativeCoShim(KvStore<K, V> store, Duration lockTimeout, Duration tombstoneTtl) {
    this(store, lockTimeout, tombstoneTtl, true);
  }

  /**
   * @param speculative false for the non-speculative baseline (see the class comment)
   */
  public SpeculativeCoShim(
      KvStore<K, V> store, Duration lockTimeout, Duration tombstoneTtl, boolean speculative) {
    this.locksMap = new LocksMap<>(speculative);
    this.store = Objects.requireNonNull(store);
    this.lockTimeout = Objects.requireNonNull(lockTimeout);
    this.tombstoneTtl = Objects.requireNonNull(tombstoneTtl);
    startBackgroundGarbageCollector();
  }

  /** pseudo.txt gc_tombstones(): removes the expired tombstones, never a live txn. */
  private void startBackgroundGarbageCollector() {
    Thread gcThread =
        new Thread(
            () -> {
              while (true) {
                try {
                  Thread.sleep(GC_PERIOD.toMillis());
                  long now = System.currentTimeMillis();
                  // CHM's entrySet().removeIf removes an entry only if it still maps to that value
                  transactionsMap
                      .entrySet()
                      .removeIf(entry -> entry.getValue().isExpiredTombstone(now));
                } catch (InterruptedException e) {
                  Thread.currentThread().interrupt();
                  break; // Exit the loop if interrupted
                }
              }
            },
            "coshim-tombstone-gc");
    gcThread.setDaemon(true); // Set as daemon so it doesn't prevent JVM shutdown
    gcThread.start();
  }

  @Override
  public Outcome start(String txnId) {
    Transaction<K, V> txn = new Transaction<>(txnId, Status.STARTED, null);
    if (transactionsMap.putIfAbsent(txnId, txn) != null) {
      return Outcome.FAILED; // already registered or tombstone
    }
    return Outcome.SUCCEEDED;
  }

  @Override
  public Outcome end(String txnId) {
    Transaction<K, V> txn = transactionsMap.get(txnId);
    if (txn == null) {
      return Outcome.FAILED;
    }
    if (!txn.compareAndSwapStatus(Status.STARTED, Status.EXECUTED)) {
      if (FINISHED.contains(txn.getStatus())) {
        return Outcome.SUCCEEDED; // duplicate end: already executed or prepared, never abort it
      }
      // aborted or must_abort: a cascade / foreign abort may have landed after the last get/put and
      // nobody released the locks yet. Idempotent
      abortTransaction(txn, AbortCause.CASCADE);
      return Outcome.FAILED;
    }
    // successors waiting in lock() for this txn to reach the speculation point
    locksMap.statusChanged(txn);
    return Outcome.SUCCEEDED;
  }

  @Override
  public V get(String txnId, K key) {
    Transaction<K, V> txn = running(txnId);
    LockResult res = locksMap.lock(key, txn, LockType.LockMode.SHARED, lockTimeout);
    if (res == LockResult.MUST_ABORT || res == LockResult.TIMED_OUT) {
      abortTransaction(txn, lockFailure(res));
      throw new TxnAbortedException("get: lock failed, must abort");
    }
    // own write first: read_buffer may hold the value read before this txn's own put
    V own = txn.getWriteSet().get(key);
    if (own != null) {
      return own;
    }
    LocksMap.ReadResult<V> readRes = locksMap.read(key, txn);
    if (readRes.isDoomed()) {
      abortTransaction(txn, AbortCause.CASCADE);
      throw new TxnAbortedException("get: read failed, must abort");
    }
    if (readRes.value() != null) {
      txn.addToReadSet(key, readRes.value());
      return readRes.value();
    }
    V value = store.get(key);
    txn.addToReadSet(key, value);
    return value;
  }

  @Override
  public void put(String txnId, K key, V value) {
    Objects.requireNonNull(value, "values are non-null");
    Transaction<K, V> txn = running(txnId);
    LockResult res = locksMap.lock(key, txn, LockType.LockMode.EXCLUSIVE, lockTimeout);
    if (res == LockResult.MUST_ABORT || res == LockResult.TIMED_OUT) {
      abortTransaction(txn, lockFailure(res));
      throw new TxnAbortedException("put: lock failed, must abort");
    }
    txn.addToWriteSet(key, value);
  }

  @Override
  public Vote prepare(String txnId) {
    Transaction<K, V> txn = transactionsMap.get(txnId);
    if (txn == null) {
      return Vote.NO;
    }
    if (VOTED_YES.contains(txn.getStatus())) {
      return Vote.YES; // duplicate prepare: re-send the vote
    }
    if (!PREPARABLE.contains(txn.getStatus())) {
      abortTransaction(txn, AbortCause.PREPARE_NO); // started, aborted (incl. tombstone) or must_abort
      return Vote.NO;
    }
    if (!locksMap.awaitPredecessorsResolved(txn)) {
      abortTransaction(txn, AbortCause.CASCADE); // doomed while waiting; a no-op if already aborted
      return Vote.NO;
    }
    if (!PREPARABLE.contains(txn.compareAndSwapStatus(PREPARABLE, Status.PREPARED))) {
      if (VOTED_YES.contains(txn.getStatus())) {
        return Vote.YES; // a concurrent duplicate prepare already voted YES
      }
      abortTransaction(txn, AbortCause.CASCADE); // a late cascade landed after the wait
      return Vote.NO;
    }
    return Vote.YES;
  }

  @Override
  public void commit(String txnId) {
    Transaction<K, V> txn = transactionsMap.get(txnId);
    if (txn == null) {
      return;
    }
    // only from prepared: a duplicate commit must not re-apply the buffer over a successor's writes
    if (!txn.compareAndSwapStatus(Status.PREPARED, Status.COMMITTING)) {
      return;
    }
    store.storeAll(txn.getWriteSet());
    locksMap.release(txn);
    txn.setStatus(Status.COMMITTED);
    transactionsMap.remove(txnId, txn);
    stats.committed();
  }

  @Override
  public void abort(String txnId) {
    Transaction<K, V> txn =
        transactionsMap.computeIfAbsent(
            txnId,
            id ->
                new Transaction<>(
                    id, Status.ABORTED, System.currentTimeMillis() + tombstoneTtl.toMillis()));
    if (txn.getStatus() == Status.ABORTED) {
      return; // tombstone (new or old), or already aborted: abort_transaction would be a no-op
    }
    abortTransaction(txn, AbortCause.COORDINATOR);
  }

  /** The txn of a get/put: FAILED (thrown) if unknown, or if the request came after end(). */
  private Transaction<K, V> running(String txnId) {
    Transaction<K, V> txn = transactionsMap.get(txnId);
    if (txn == null) {
      throw new TxnAbortedException("txn " + txnId + " is unknown");
    }
    if (FINISHED.contains(txn.getStatus())) {
      // never abort here: a prepared txn voted YES
      throw new TxnAbortedException("txn " + txnId + " already ended");
    }
    return txn; // aborted / must_abort: lock() returns MUST_ABORT
  }

  /** Commit and abort counters, by abort cause. */
  public ShimStats stats() {
    return stats;
  }

  private static AbortCause lockFailure(LockResult res) {
    // MUST_ABORT of a txn that was not marked: it lost an upgrade race (a marked one counts as cascade)
    return res == LockResult.TIMED_OUT ? AbortCause.LOCK_TIMEOUT : AbortCause.UPGRADE_CONFLICT;
  }

  /**
   * @param cause why the caller aborts txn; a txn that was must_abort counts as a cascade whatever
   *     the site that finally aborts it
   */
  private void abortTransaction(Transaction<K, V> txn, AbortCause cause) {
    Status before = txn.compareAndSwapStatus(ABORTABLE, Status.ABORTED);
    if (!ABORTABLE.contains(before)) {
      return; // already aborted or committing/committed
    }
    stats.aborted(before == Status.MUST_ABORT ? AbortCause.CASCADE : cause);
    List<Transaction<K, V>> doomed = locksMap.abortAndRelease(txn);
    transactionsMap.remove(txn.getTransactionId(), txn);
    for (Transaction<K, V> t : doomed) {
      abortTransaction(t, AbortCause.CASCADE);
    }
  }
}
