package ch.epfl.coshim.core;

import ch.epfl.coshim.store.KvStore;
import ch.epfl.coshim.types.LockType;
import ch.epfl.coshim.types.LocksMap;
import ch.epfl.coshim.types.LocksMap.LockResult;
import ch.epfl.coshim.types.Transaction;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The speculative CO-compliant shim of pseudo.txt (S2PL + speculation at {@code executed}).
 *
 * <p>TODO: not implemented yet. This template only wires it into Seata; translate pseudo.txt here
 * (LockNode, chain, locks_map, transactions_map, lock/upgrade/abort_transaction/mark_must_abort),
 * including the tombstones for an abort that arrives before start (see {@link CoShim} and {@link
 * NoCcShim} for a reference of that part).
 */
public class SpeculativeCoShim<K, V> implements CoShim<K, V> {
  private static final Duration TOMBSTONE_TTL = Duration.ofSeconds(10);
  private static final Duration LOCK_TIMEOUT = Duration.ofSeconds(10);

  private final KvStore<K, V> store;
  private final LocksMap<K, V> locksMap = new LocksMap<>();
  private final Map<String, Transaction<K, V>> transactionsMap = new ConcurrentHashMap<>();

  public SpeculativeCoShim(KvStore<K, V> store) {
    this.store = Objects.requireNonNull(store);
    // Launch background garbage collector
    startBackgroundGarbageCollector();
  }

  private void startBackgroundGarbageCollector() {
    Thread gcThread =
        new Thread(
            () -> {
              while (true) {
                try {
                  Thread.sleep(1000); // Sleep for 1 second
                  long now = System.currentTimeMillis();
                  transactionsMap
                      .entrySet()
                      .removeIf(entry -> entry.getValue().getExpirationTime() < now);
                } catch (InterruptedException e) {
                  Thread.currentThread().interrupt();
                  break; // Exit the loop if interrupted
                }
              }
            });
    gcThread.setDaemon(true); // Set as daemon so it doesn't prevent JVM shutdown
    gcThread.start();
  }

  @Override
  public Outcome start(String txnId) {
    Transaction<K, V> txn =
        new Transaction<>(txnId, Transaction.Status.STARTED, LOCK_TIMEOUT.toMillis());
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
    if (!txn.compareAndSwapStatus(Transaction.Status.STARTED, Transaction.Status.EXECUTED)) {
      if (Set.of(
              Transaction.Status.EXECUTED,
              Transaction.Status.PREPARED,
              Transaction.Status.COMMITTING,
              Transaction.Status.COMMITTED)
          .contains(txn.getStatus()))
        return Outcome.SUCCEEDED; // duplicate end: already executed or prepared, never abort it

      // TODO: check if it is this one
      // abort_transaction
      abortTransaction(txn);
      return Outcome.FAILED;
    }
    return Outcome.SUCCEEDED;
  }

  @Override
  public V get(String txnId, K key) {
    Transaction<K, V> txn = transactionsMap.get(txnId);
    if (txn == null) return null;
    if (Set.of(
            Transaction.Status.EXECUTED,
            Transaction.Status.PREPARED,
            Transaction.Status.COMMITTING,
            Transaction.Status.COMMITTED,
            Transaction.Status.ABORTED)
        .contains(txn.getStatus())) {
      return null;
    }
    LockResult res = locksMap.lock(key, txn, LockType.LockMode.SHARED, LOCK_TIMEOUT);
    if (res == LockResult.MUST_ABORT || res == LockResult.TIMED_OUT) {
      abortTransaction(txn);
      throw new TxnAbortedException("get: lock failed, must abort");
    }
    Map<K, V> readSet = txn.getReadSet();
    if (readSet.containsKey(key)) {
      return readSet.get(key);
    }
    LocksMap.ReadResult<V> readRes = locksMap.read(key, txn);
    if (readRes.isDoomed()) {
      abortTransaction(txn);
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
    Transaction<K, V> txn = transactionsMap.get(txnId);
    if (txn == null) return;
    if (Set.of(
            Transaction.Status.EXECUTED,
            Transaction.Status.PREPARED,
            Transaction.Status.COMMITTING,
            Transaction.Status.COMMITTED,
            Transaction.Status.ABORTED)
        .contains(txn.getStatus())) {
      return;
    }
    LockResult res = locksMap.lock(key, txn, LockType.LockMode.EXCLUSIVE, LOCK_TIMEOUT);
    if (res == LockResult.MUST_ABORT || res == LockResult.TIMED_OUT) {
      abortTransaction(txn);
      throw new TxnAbortedException("put: lock failed, must abort");
    }
    txn.addToWriteSet(key, value);
  }

  @Override
  public Vote prepare(String txnId) {
    Transaction<K, V> txn = transactionsMap.get(txnId);
    if (txn == null) return Vote.NO;
    if (Set.of(
            Transaction.Status.PREPARED,
            Transaction.Status.COMMITTING,
            Transaction.Status.COMMITTED)
        .contains(txn.getStatus())) return Vote.YES; // already prepared or committed

    if (!Set.of(Transaction.Status.PREPARED, Transaction.Status.EXECUTED)
        .contains(txn.getStatus())) {
      // TODO: check if abort_transaction
      abortTransaction(txn);
      return Vote.NO; // must abort
    }

    if (!locksMap.awaitPredecessorsResolved(txn)) {
      // TODO: check if abort_transaction
      abortTransaction(txn);
      return Vote.NO;
    }
    if (txn.compareAndSwapStatus(
        Set.of(Transaction.Status.PREPARED, Transaction.Status.EXECUTED),
        Transaction.Status.PREPARED)) {
      if (Set.of(Transaction.Status.COMMITTING, Transaction.Status.COMMITTED)
          .contains(txn.getStatus())) {
        return Vote.YES; // already prepared or committed
      }
      abortTransaction(txn);
      return Vote.NO;
    }
    return Vote.YES;
  }

  @Override
  public void commit(String txnId) {
    Transaction<K, V> txn = transactionsMap.get(txnId);
    if (txn == null) return;
    if (!txn.compareAndSwapStatus(
        Set.of(Transaction.Status.PREPARED, Transaction.Status.COMMITTING),
        Transaction.Status.COMMITTING)) {
      return;
    }
    store.storeAll(txn.getWriteSet());
    locksMap.release(txn);
    txn.setStatus(Transaction.Status.COMMITTED);
    transactionsMap.remove(txnId, txn);
  }

  @Override
  public void abort(String txnId) {
    Transaction<K, V> txn =
        transactionsMap.computeIfAbsent(
            txnId,
            id -> new Transaction<>(id, Transaction.Status.ABORTED, TOMBSTONE_TTL.toMillis()));
    if (txn.getStatus() == Transaction.Status.ABORTED) {
      return; // already aborted or tombstone
    }
    abortTransaction(txn);
  }

  private void abortTransaction(Transaction<K, V> txn) {
    if (!txn.compareAndSwapStatus(
        Set.of(
            Transaction.Status.STARTED,
            Transaction.Status.EXECUTED,
            Transaction.Status.MUST_ABORT,
            Transaction.Status.PREPARED),
        Transaction.Status.ABORTED)) {
      return; // already aborted or committing/committed
    }
    List<Transaction<K, V>> doomed = locksMap.abortAndRelease(txn);
    transactionsMap.remove(txn.getId(), txn);
    for (Transaction<K, V> t : doomed) {
      abortTransaction(t);
    }
  }
}
