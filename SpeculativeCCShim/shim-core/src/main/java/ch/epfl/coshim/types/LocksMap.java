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
 * transactions_map; every step of get()/lock()/upgrade()/prepare()/commit()/abort_transaction()
 * that touches the chains is here.
 *
 * <h2>Latching: per-key instead of pseudo.txt's global atomic(locks_map)</h2>
 *
 * <p>Every change to the chain of key k runs inside {@code locksMap.compute(k, ...)} (or
 * computeIfPresent), so it is atomic with the creation and the removal of k's chain: a concurrent
 * request sees the chain or none, exactly as pseudo.txt's "remove k from locks_map, under the latch".
 * An emptied chain is removed by returning null from that same computation. At most one map entry
 * is held at a time; inside it the chain's monitor and then a txn monitor (lock order: entry, chain,
 * txn). Waits sleep on the chain's monitor only.
 *
 * <p>Six of pseudo.txt's eight atomic(locks_map) blocks touch a single key (get, lock, upgrade, and
 * the re-reads of the lock/upgrade/prepare wait loops), so a per-key latch is exactly the same
 * critical section. The status re-check of lock()/upgrade() is atomic with the update of
 * locks_acquired because both happen under the txn's monitor, where abort_transaction also CASes the
 * status (the condition pseudo.txt states for per-key latches). The two multi-key blocks are split key
 * by key, which is safe:
 *
 * <ul>
 *   <li>commit(): the store writes of all keys happen before any release, so a successor released
 *       early on k1 already sees txn's values in the store; on a key where it is still behind txn it
 *       still waits. Nothing waits for "txn left every chain" (under the global latch too, a successor
 *       could commit before txn's status became committed).
 *   <li>abort_transaction(): the status is CASed to aborted before any key is touched, so on a key
 *       not processed yet txn's node satisfies no wait and every successor stays blocked behind it.
 *       On each key, the cascade marks the dependents before the splice in the same critical section,
 *       so a dependent can only get past txn's node (and vote) after it was marked: cascade before
 *       splice holds per key. A prepared txn is never marked (the CAS excludes it), and none can depend
 *       on txn: its prepare needs txn's node gone first.
 * </ul>
 *
 * <p>prepare() was already "latch, wait, latch" key by key in pseudo.txt. That is sound because once
 * txn's node is the head of a chain nothing can be put in front of it again: lock() appends, and
 * upgrade() inserts only behind the upgrader's own node.
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

  /**
   * Result of get()'s speculative read: either txn is doomed (abort it), or {@code value} is what it
   * reads, null meaning "no predecessor version: read the store".
   */
  public static record ReadResult<V>(boolean isDoomed, V value) {
    static <V> ReadResult<V> doomed() {
      return new ReadResult<>(true, null);
    }

    static <V> ReadResult<V> of(V value) {
      return new ReadResult<>(false, value);
    }
  }

  private final ConcurrentHashMap<K, Chain<K, V>> locksMap = new ConcurrentHashMap<>();
  /** What lock()/upgrade() wait for: every predecessor holder in this set. */
  private final Set<Status> lockReady;
  private final boolean speculative;

  /** The speculative lock table of pseudo.txt. */
  public LocksMap() {
    this(true);
  }

  /**
   * @param speculative true: lock() waits until the predecessors are executed (pseudo.txt). false:
   *     the non-speculative S2PL baseline, lock() waits until they committed, so no txn ever reads
   *     an uncommitted value and an abort never cascades
   */
  public LocksMap(boolean speculative) {
    this.speculative = speculative;
    this.lockReady = speculative ? SPECULATION_POINT : RESOLVED;
  }

  /**
   * lock(k, mode, txn) and upgrade(k, txn): takes the lock, then waits (at most {@code timeout})
   * until every holder of its predecessor is at least executed (committed if not speculative). On
   * MUST_ABORT / TIMED_OUT the caller runs abort_transaction(txn).
   */
  public LockResult lock(K key, Transaction<K, V> txn, LockMode mode, Duration timeout) {
    long deadline = System.nanoTime() + timeout.toNanos();
    LockType<K, V> held = txn.getLock(key);
    if (held == null) {
      Chain<K, V> chain = enqueue(key, txn, mode);
      return chain == null ? LockResult.MUST_ABORT : await(chain, txn, deadline);
    }
    if (held.mode() == LockMode.EXCLUSIVE || mode == LockMode.SHARED) {
      return txn.isRunning() ? LockResult.ALREADY_HELD : LockResult.MUST_ABORT;
    }
    List<Chain.UpgradeResult> result = new ArrayList<>(1);
    Chain<K, V> chain = locksMap.computeIfPresent(key, (k, c) -> {
      result.add(c.upgrade(txn));
      return c;   // an upgrade never empties the chain
    });
    if (chain == null) {
      // txn holds k, so its chain exists unless a foreign abort_transaction (coordinator abort, or a
      // cascade's eager abort) released txn's locks meanwhile and the chain became empty. txn is
      // aborted then: creating a chain for it would leave a node nobody removes
      return LockResult.MUST_ABORT;
    }
    return switch (result.get(0)) {
      case MUST_ABORT -> LockResult.MUST_ABORT;
      case UPGRADED -> LockResult.ACQUIRED;
      case UPGRADED_MUST_WAIT -> await(chain, txn, deadline);
    };
  }

  /** get()'s read under the latch, for a txn that holds key (lock() returned ACQUIRED/ALREADY_HELD). */
  public ReadResult<V> read(K key, Transaction<K, V> txn) {
    Chain<K, V> chain = locksMap.get(key);
    // no chain: a foreign abort_transaction released txn's locks (as in lock()), so txn is aborted
    return chain == null ? ReadResult.doomed() : chain.read(txn);
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
      // no chain: txn's locks were released by a foreign abort_transaction, as in lock()
      if (chain == null || chain.awaitPredecessor(txn, RESOLVED, false, 0) != Chain.WaitResult.READY) {
        return false;
      }
    }
    return true;
  }

  /**
   * commit()'s release of every lock of txn (status committing), one key at a time. The caller has
   * already applied txn's write buffer to the store.
   */
  public void release(Transaction<K, V> txn) {
    for (Map.Entry<K, LockType<K, V>> e : txn.frozenLocks().entrySet()) {
      LockNode<K, V> node = e.getValue().node();
      locksMap.computeIfPresent(e.getKey(), (k, c) -> c.release(txn, node) ? null : c);
    }
  }

  /**
   * abort_transaction()'s part on the chains, for a txn the caller just CASed to aborted: on every
   * key, cascades must_abort to the dependents and then releases, one key at a time. Wakes the newly
   * doomed txns afterwards, outside any latch.
   *
   * @return the doomed txns that had already finished executing: the caller runs
   *     abort_transaction on each of them (no thread is left to do it)
   */
  public List<Transaction<K, V>> abortAndRelease(Transaction<K, V> txn) {
    List<Transaction<K, V>> marked = new ArrayList<>();
    List<Transaction<K, V>> doomedFinished = new ArrayList<>();
    for (Map.Entry<K, LockType<K, V>> e : txn.frozenLocks().entrySet()) {
      LockType<K, V> held = e.getValue();
      locksMap.computeIfPresent(
          e.getKey(),
          (k, c) -> c.abortAndRelease(txn, held, speculative, marked, doomedFinished) ? null : c);
    }
    for (Transaction<K, V> t : marked) {
      statusChanged(t);
    }
    return doomedFinished;
  }

  /**
   * Wakes the waiters on every key txn holds, so they re-check txn's status. Call after every
   * successful status CAS (txn has left started by then), outside any latch.
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
    boolean[] acquired = {false};
    Chain<K, V> chain = locksMap.compute(key, (k, c) -> {
      Chain<K, V> target = c != null ? c : new Chain<>(k);
      acquired[0] = target.acquire(txn, mode);
      return acquired[0] ? target : c;   // not running: leave the map as it was
    });
    return acquired[0] ? chain : null;
  }

  Chain<K, V> getChain(K key) {
    return locksMap.get(key);
  }

  private LockResult await(Chain<K, V> chain, Transaction<K, V> txn, long deadline) {
    return switch (chain.awaitPredecessor(txn, lockReady, true, deadline)) {
      case READY -> LockResult.ACQUIRED;
      case DOOMED -> LockResult.MUST_ABORT;
      case TIMED_OUT -> LockResult.TIMED_OUT;
    };
  }
}
