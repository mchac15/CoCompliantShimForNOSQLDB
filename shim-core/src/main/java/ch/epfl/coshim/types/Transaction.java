package ch.epfl.coshim.types;

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A txn of pseudo.txt. Its monitor ({@code synchronized (txn)}) guards {@code status} and {@code
 * locks_acquired}. Only a running (started) txn acquires locks, and the {@link Chain} checks that
 * under this monitor, so locks_acquired is frozen as soon as the status leaves started: commit and
 * abort iterate it without copying. Lock order: a chain latch first, then the txn monitor, never
 * the reverse.
 */
public class Transaction<K, V> {
  private final String transactionId;
  private final Map<K, LockType<K, V>> locksAcquired;
  private final Map<K, V> writeSet;
  private final Map<K, V> readSet;
  /** expires_at of pseudo.txt, in {@link System#currentTimeMillis()}: set only on a tombstone. */
  private final Long expiresAtMillis;
  /** Age for deadlock prevention (smaller = older); see {@link LockNode#hasYoungerHolderNotIn}. */
  private final long ts;
  private Status status;
  /**
   * Has ever been executed (guarded by the monitor). A txn that never was has no reader of its
   * write buffer: no successor gets past its node before it reaches the speculation point.
   */
  private boolean reachedExecuted;
  /** Doomed by wound-wait ({@link #wound()}), not by a cascade: statistics only. */
  private boolean wounded;
  /**
   * Statistics only (a {@code ShimStats.AbortCause}; this package does not depend on core): why the
   * txn aborted, and the cause of the abort at the root of its cascade tree (its own cause unless a
   * cascade doomed it). The root is copied into every txn this one's abort marks, so a cascade victim
   * knows what started its tree however deep it is.
   */
  private Enum<?> abortCause;
  private Enum<?> rootCause;
  /**
   * Speculation limiter (fanout): the txns this one counted itself as a speculative dependent of, each
   * once. Guarded by the monitor; only added to while started, emptied when it resolves.
   */
  private final Set<Transaction<K, V>> dependsOn = new HashSet<>();
  /** Speculation limiter (fanout): the running txns counted in their {@code dependsOn}. */
  private final AtomicInteger specDependents = new AtomicInteger();

  /** A lock granted while the txns ahead on its chain were not all committed yet (speculation). */
  private record Grant(long atNanos, List<? extends Transaction<?, ?>> pending) {}

  // statistics only (ShimStats), never read by the protocol
  /** When the txn committed or aborted, in {@link System#nanoTime()}; 0 while unresolved. */
  private volatile long resolvedAtNanos;
  private final List<Grant> grants = new ArrayList<>();
  private int maxDepth;
  private int cascadeFanout;
  private int sparedMarks;
  /** When this txn was created, in {@link System#nanoTime()}. */
  private final long createdAtNanos = System.nanoTime();
  /** How long prepare() waited for its predecessors to commit. */
  private long prepareWaitNanos;
  /** Time spent inside lock()/upgrade() so far. */
  private long lockWaitNanos;
  /** When prepare() voted YES, in {@link System#nanoTime()}; 0 before. */
  private long votedAtNanos;
  /** lock()/upgrade() calls whose speculative grant the limiter refused (first reason), and their wait. */
  private int limitedByDepth;
  private int limitedByFanout;
  private long limitedWaitNanos;

  /** @param expiresAtMillis null for a live txn, the absolute expiry time for a tombstone */
  public Transaction(String transactionId, Status status, Long expiresAtMillis) {
    this(transactionId, status, expiresAtMillis, 0);
  }

  /** @param ts the txn's age; txns of equal ts never abort each other for deadlock prevention */
  public Transaction(String transactionId, Status status, Long expiresAtMillis, long ts) {
    this.transactionId = transactionId;
    this.locksAcquired = new HashMap<>();
    this.status = status;
    this.writeSet = new HashMap<>();
    this.readSet = new HashMap<>();
    this.expiresAtMillis = expiresAtMillis;
    this.ts = ts;
  }

  public long getTs() {
    return ts;
  }

  public String getTransactionId() {
    return transactionId;
  }

  public synchronized LockType<K, V> getLock(K key) {
    return locksAcquired.get(key);
  }

  /** Callers hold this monitor and checked {@link #isRunning()} under it. */
  void putLock(K key, LockType<K, V> lockType) {
    assert Thread.holdsLock(this) && status == Status.STARTED;
    locksAcquired.put(key, lockType);
  }

  /**
   * locks_acquired, read-only and without copying: once the status left started nothing adds to it
   * anymore. Reading the status under the monitor also makes every earlier putLock visible.
   */
  Map<K, LockType<K, V>> frozenLocks() {
    synchronized (this) {
      if (status == Status.STARTED) {
        throw new IllegalStateException("txn " + transactionId + " is still running");
      }
    }
    return Collections.unmodifiableMap(locksAcquired);
  }

  public synchronized Status getStatus() {
    return status;
  }

  public synchronized boolean isRunning() {
    return status == Status.STARTED;
  }

  /** aborted or must_abort. */
  public synchronized boolean isDoomed() {
    return status == Status.ABORTED || status == Status.MUST_ABORT;
  }

  public Map<K, V> getWriteSet() {
    return writeSet;
  }

  public Map<K, V> getReadSet() {
    return readSet;
  }

  /** A tombstone past its expires_at (pseudo.txt gc_tombstones()); always false for a live txn. */
  public boolean isExpiredTombstone(long nowMillis) {
    return expiresAtMillis != null && nowMillis >= expiresAtMillis;
  }

  public void addToWriteSet(K key, V value) {
    writeSet.put(key, value);
  }

  public void addToReadSet(K key, V value) {
    readSet.put(key, value);
  }

  public void setStatus(Status newStatus) {
    synchronized (this) {
      this.status = newStatus;
      reachedExecuted |= newStatus == Status.EXECUTED;
    }
  }

  /**
   * CAS_returning of pseudo.txt: moves to {@code to} only if the current status is in {@code from}.
   * Returns the status before the call; the CAS succeeded iff {@code from} contains it. After a
   * successful CAS, call {@link LocksMap#statusChanged} so that waiters re-check.
   */
  public synchronized Status compareAndSwapStatus(Set<Status> from, Status to) {
    Status before = status;
    if (from.contains(before)) {
      status = to;
      reachedExecuted |= to == Status.EXECUTED;
    }
    return before;
  }

  public boolean compareAndSwapStatus(Status from, Status to) {
    return compareAndSwapStatus(EnumSet.of(from), to) == from;
  }

  /**
   * Wound-wait's mark: started → must_abort, so the txn can no longer reach executed (nor vote).
   * The caller then runs abort_transaction on it outside any latch.
   *
   * @return false if it was not started (nothing changed): someone else already doomed it, or it
   *     executed and no longer blocks a lock() wait
   */
  synchronized boolean wound() {
    if (status != Status.STARTED) {
      return false;
    }
    status = Status.MUST_ABORT;
    wounded = true;
    return true;
  }

  public synchronized boolean isWounded() {
    return wounded;
  }

  /**
   * A cascade's mark: as {@link #compareAndSwapStatus(Set, Status)} to must_abort, and on success
   * records {@code root} (the doomer's root cause) in the same step, so whoever later aborts this txn
   * finds it.
   */
  synchronized Status markMustAbort(Set<Status> from, Enum<?> root) {
    Status before = compareAndSwapStatus(from, Status.MUST_ABORT);
    if (from.contains(before)) {
      rootCause = root;
    }
    return before;
  }

  /** Statistics only: set by abort_transaction before it releases (and cascades). */
  public synchronized void setAbortCauses(Enum<?> cause, Enum<?> root) {
    abortCause = cause;
    rootCause = root;
  }

  /** Why the txn aborted; null while it has not. */
  public synchronized Enum<?> abortCause() {
    return abortCause;
  }

  /** The root cause of its cascade tree: set by the mark of a cascade, or by its own abort. */
  public synchronized Enum<?> rootCause() {
    return rootCause;
  }

  /**
   * The fanout cap's admission, under the caller's chain monitor: counts this txn as a dependent of
   * every txn of {@code on} it does not depend on yet, all or nothing. Touches only their counters,
   * never their monitors (lock order: chain, then a single txn).
   *
   * @param full receives the txn that already had {@code max} dependents, on refusal
   * @return false if one of them is full (nothing changed); true otherwise, also when this txn is no
   *     longer started (nothing recorded: it is doomed, its wait loop sees it)
   */
  synchronized boolean addSpecDependencies(
      List<Transaction<K, V>> on, int max, List<Transaction<K, V>> full) {
    if (status != Status.STARTED) {
      return true;
    }
    List<Transaction<K, V>> added = new ArrayList<>();
    for (Transaction<K, V> t : on) {
      if (dependsOn.contains(t)) {
        continue;
      }
      if (!t.tryAddSpecDependent(max)) {
        // no wake-up for the decrements: a txn refused meanwhile on t retries at t's next change
        for (Transaction<K, V> a : added) {
          a.specDependents.decrementAndGet();
          dependsOn.remove(a);
        }
        full.add(t);
        return false;
      }
      dependsOn.add(t);
      added.add(t);
    }
    return true;
  }

  private boolean tryAddSpecDependent(int max) {
    while (true) {
      int n = specDependents.get();
      if (n >= max) {
        return false;
      }
      if (specDependents.compareAndSet(n, n + 1)) {
        return true;
      }
    }
  }

  /**
   * Once resolved (status no longer started, so nothing adds to it anymore): drops this txn from the
   * dependents of the txns it depended on, and returns them (their waiters may now fit).
   */
  List<Transaction<K, V>> clearSpecDependencies() {
    List<Transaction<K, V>> out;
    synchronized (this) {
      out = new ArrayList<>(dependsOn);
      dependsOn.clear();
    }
    for (Transaction<K, V> t : out) {
      t.specDependents.decrementAndGet();
    }
    return out;
  }

  /** The running txns counted as speculative dependents of this one (fanout cap). */
  public int specDependents() {
    return specDependents.get();
  }

  /** Statistics only: one lock()/upgrade() call the limiter held back, for {@code waitNanos}. */
  synchronized void recordLimited(boolean byDepth, long waitNanos) {
    if (byDepth) {
      limitedByDepth++;
    } else {
      limitedByFanout++;
    }
    limitedWaitNanos += waitNanos;
  }

  public synchronized int limitedByDepth() {
    return limitedByDepth;
  }

  public synchronized int limitedByFanout() {
    return limitedByFanout;
  }

  public synchronized long limitedWaitNanos() {
    return limitedWaitNanos;
  }

  /** Was ever executed: only then can a successor have read its write buffer. */
  public synchronized boolean hasReachedExecuted() {
    return reachedExecuted;
  }

  /**
   * A lock() wait ended (READY) with {@code depth} nodes ahead holding the txns {@code pending}, none
   * of them committed yet: without speculation the wait would have lasted until they resolved.
   */
  synchronized void recordGrant(int depth, List<? extends Transaction<?, ?>> pending) {
    maxDepth = Math.max(maxDepth, depth);
    if (!pending.isEmpty()) {
      grants.add(new Grant(System.nanoTime(), pending));
    }
  }

  /** The deepest dependency chain ahead of the txn at any of its lock grants (0: never speculated). */
  public synchronized int maxDepth() {
    return maxDepth;
  }

  public synchronized int speculativeGrants() {
    return grants.size();
  }

  /**
   * Estimate of the time speculation saved this txn: replays its speculative grants in order, as if
   * each had waited until the last txn it depended on resolved (plain S2PL), shifting the later ones
   * by the waits before them. Call once every predecessor resolved (prepare voted YES); one still
   * unresolved counts as resolving now.
   */
  public synchronized long savedNanos() {
    long now = System.nanoTime();
    long shift = 0;
    for (Grant g : grants) {
      long latest = 0;
      for (Transaction<?, ?> t : g.pending()) {
        long r = t.resolvedAtNanos;
        latest = Math.max(latest, r == 0 ? now : r);
      }
      shift += Math.max(0, latest - (g.atNanos() + shift));
    }
    return shift;
  }

  /**
   * Committed or aborted: stamps the time and drops the grants, so a resolved txn no longer keeps
   * its predecessors (and theirs, transitively) reachable.
   */
  public synchronized void markResolved() {
    resolvedAtNanos = System.nanoTime();
    grants.clear();
  }

  public long createdAtNanos() {
    return createdAtNanos;
  }

  public synchronized long prepareWaitNanos() {
    return prepareWaitNanos;
  }

  public synchronized void setPrepareWaitNanos(long nanos) {
    prepareWaitNanos = nanos;
  }

  public synchronized long lockWaitNanos() {
    return lockWaitNanos;
  }

  public synchronized void addLockWaitNanos(long nanos) {
    lockWaitNanos += nanos;
  }

  public synchronized long votedAtNanos() {
    return votedAtNanos;
  }

  public synchronized void setVotedAtNanos(long nanos) {
    votedAtNanos = nanos;
  }

  /** How many txns the abort of this one marked must_abort directly (cascade fan-out). */
  public synchronized int cascadeFanout() {
    return cascadeFanout;
  }

  synchronized void setCascadeFanout(int n) {
    cascadeFanout = n;
  }

  /**
   * The txns this abort met behind it but spared: on every key it met them on they were still blocked
   * in lock(), so they had read nothing this txn (nor a txn behind it there) wrote. Not counted in
   * {@link #cascadeFanout()}.
   */
  public synchronized int sparedMarks() {
    return sparedMarks;
  }

  synchronized void setSparedMarks(int n) {
    sparedMarks = n;
  }

  public static enum Status {
    STARTED,
    EXECUTED,
    PREPARED,
    COMMITTED,
    ABORTED,
    COMMITTING,
    MUST_ABORT
  }
}
