package ch.epfl.coshim.types;

import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

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
  private Status status;

  /** @param expiresAtMillis null for a live txn, the absolute expiry time for a tombstone */
  public Transaction(String transactionId, Status status, Long expiresAtMillis) {
    this.transactionId = transactionId;
    this.locksAcquired = new HashMap<>();
    this.status = status;
    this.writeSet = new HashMap<>();
    this.readSet = new HashMap<>();
    this.expiresAtMillis = expiresAtMillis;
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
    }
    return before;
  }

  public boolean compareAndSwapStatus(Status from, Status to) {
    return compareAndSwapStatus(EnumSet.of(from), to) == from;
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
