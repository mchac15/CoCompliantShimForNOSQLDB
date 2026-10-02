package ch.epfl.coshim.types;

import ch.epfl.coshim.types.LockType.LockMode;
import ch.epfl.coshim.types.Transaction.Status;
import java.time.Duration;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * locks_map of pseudo.txt, and the shim's only entry point to the lock table: the shim never sees a
 * {@link Chain} or reads a {@link LockNode}. It keeps the status CASes, the buffers and
 * transactions_map; every step of lock()/upgrade()/prepare()/commit()/abort_transaction() that
 * touches the chains is here.
 *
 * <p>One chain per key, created with computeIfAbsent. An emptied chain retires itself under its
 * latch and is removed from the map; a request that fetched a retired chain removes it if still
 * mapped and retries, so a key never has two live chains. A chain that still has nodes is never
 * retired, so {@code locksMap.get(k)} is the right chain for every key a txn holds a lock on.
 *
 * <p>At most one chain latch is held at a time.
 */
public class LocksMap<K, V> {
  private static final Set<Status> SPECULATION_POINT =
      EnumSet.of(Status.EXECUTED, Status.PREPARED, Status.COMMITTING, Status.COMMITTED);
  private static final Set<Status> RESOLVED = EnumSet.of(Status.COMMITTED);

  public static enum LockResult {
    /** Acquired (or upgraded), and every predecessor holder reached the speculation point. */
    ACQUIRED,
    /** txn already holds the key in this mode or a stronger one. */
    ALREADY_HELD,
    /** txn is doomed or no longer running, or lost an RMW-vs-RMW upgrade: abort it. */
    MUST_ABORT,
    /** lock_timeout expired while waiting (assumed deadlock): abort it. */
    TIMED_OUT
  }

  private final Map<K, Chain<K, V>> locksMap;

  public LocksMap() {
    this.locksMap = new ConcurrentHashMap<>();
  }

  /**
   * lock(k, mode, txn) and upgrade(k, txn): takes the lock, then waits (at most {@code timeout})
   * until every holder of its predecessor is at least executed. On MUST_ABORT / TIMED_OUT the
   * caller runs abort_transaction(txn).
   */
  public LockResult lock(K key, Transaction<K, V> txn, LockMode mode, Duration timeout) {
    long deadline = System.nanoTime() + timeout.toNanos();
    LockType<K, V> held = txn.getLock(key);
    if (held != null) {
      if (held.mode() == LockMode.EXCLUSIVE || mode == LockMode.SHARED) {
        return txn.isRunning() ? LockResult.ALREADY_HELD : LockResult.MUST_ABORT;
      }
      Chain<K, V> chain = locksMap.get(key);
      if (chain == null) {
        return LockResult.MUST_ABORT;   // a foreign abort_transaction already released txn's locks
      }
      switch (chain.upgrade(txn)) {
        case MUST_ABORT:
          return LockResult.MUST_ABORT;
        case UPGRADED:
          return LockResult.ACQUIRED;
        default:
          return await(chain, txn, deadline);
      }
    }
    Chain<K, V> chain = enqueue(key, txn, mode);
    return chain == null ? LockResult.MUST_ABORT : await(chain, txn, deadline);
  }

  /**
   * prepare()'s wait: for every lock txn holds, waits (unbounded) until every holder of its
   * predecessor committed. txn must have left started (it is executed).
   *
   * @return true if every predecessor resolved, false if txn got aborted / doomed meanwhile
   */
  public boolean awaitPredecessorsResolved(Transaction<K, V> txn) {
    for (K key : txn.frozenLocks().keySet()) {
      Chain<K, V> chain = locksMap.get(key);
      // null: a foreign abort_transaction already released txn's locks
      if (chain == null || chain.awaitPredecessor(txn, RESOLVED, false, 0) != Chain.WaitResult.READY) {
        return false;
      }
    }
    return true;
  }

  /** commit()'s release of every lock of txn (status committing), one key latch at a time. */
  public void release(Transaction<K, V> txn) {
    for (Map.Entry<K, LockType<K, V>> e : txn.frozenLocks().entrySet()) {
      Chain<K, V> chain = locksMap.get(e.getKey());
      if (chain.release(txn, e.getValue().node())) {
        locksMap.remove(e.getKey(), chain);
      }
    }
  }

  /**
   * abort_transaction()'s part on the chains, for a txn the caller just CASed to aborted: on every
   * key, cascades must_abort to the dependents and then releases, one key latch at a time. Wakes the
   * newly doomed txns afterwards, outside any latch.
   *
   * @return the doomed txns that had already finished executing: the caller runs
   *     abort_transaction on each of them (no thread is left to do it)
   */
  public List<Transaction<K, V>> abortAndRelease(Transaction<K, V> txn) {
    List<Transaction<K, V>> marked = new ArrayList<>();
    List<Transaction<K, V>> doomedFinished = new ArrayList<>();
    for (Map.Entry<K, LockType<K, V>> e : txn.frozenLocks().entrySet()) {
      Chain<K, V> chain = locksMap.get(e.getKey());
      if (chain.abortAndRelease(txn, e.getValue(), marked, doomedFinished)) {
        locksMap.remove(e.getKey(), chain);
      }
    }
    for (Transaction<K, V> t : marked) {
      statusChanged(t);
    }
    return doomedFinished;
  }

  /**
   * Wakes the waiters on every key txn holds, so they re-check txn's status. Call after every
   * successful status CAS (txn has left started by then), outside any chain latch.
   */
  public void statusChanged(Transaction<K, V> txn) {
    for (K key : txn.frozenLocks().keySet()) {
      Chain<K, V> chain = locksMap.get(key);
      if (chain != null) {   // txn may already have released this key
        chain.signalAll();
      }
    }
  }

  public boolean containsKey(K key) {
    return locksMap.containsKey(key);
  }

  /** lock()'s critical section without the wait: the chain txn was appended to, or null. */
  Chain<K, V> enqueue(K key, Transaction<K, V> txn, LockMode mode) {
    while (true) {
      Chain<K, V> chain = locksMap.computeIfAbsent(key, Chain::new);
      switch (chain.acquire(txn, mode)) {
        case ACQUIRED:
          return chain;
        case NOT_RUNNING:
          return null;
        default:
          locksMap.remove(key, chain);   // retired: drop it if still mapped, then retry
      }
    }
  }

  Chain<K, V> getChain(K key) {
    return locksMap.get(key);
  }

  private LockResult await(Chain<K, V> chain, Transaction<K, V> txn, long deadline) {
    switch (chain.awaitPredecessor(txn, SPECULATION_POINT, true, deadline)) {
      case READY:
        return LockResult.ACQUIRED;
      case DOOMED:
        return LockResult.MUST_ABORT;
      default:
        return LockResult.TIMED_OUT;
    }
  }
}
