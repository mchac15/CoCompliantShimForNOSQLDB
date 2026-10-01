package ch.epfl.coshim.types;

import java.sql.Timestamp;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * A txn of pseudo.txt. Its monitor ({@code synchronized (txn)}) guards {@code status} and {@code
 * locks_acquired}, so that the status re-check in lock()/upgrade() and the update of
 * locks_acquired are atomic with respect to abort_transaction(): it moves the status to aborted
 * under this monitor and only then reads {@link #locksSnapshot()}. Lock order: a chain latch first,
 * then the txn monitor, never the reverse.
 */
public class Transaction<K, V> {
  private final String transactionId;
  private final Map<K, LockType<K, V>> locksAcquired;
  private final Map<K, V> writeSet;
  private final Map<K, V> readSet;
  private final Timestamp expirationTime;
  private Status status;

  public Transaction(String transactionId, Status status, Timestamp expirationTime) {
    this.transactionId = transactionId;
    this.locksAcquired = new HashMap<>();
    this.status = status;
    this.writeSet = new HashMap<>();
    this.readSet = new HashMap<>();
    this.expirationTime = expirationTime;
  }

  public String getTransactionId() {
    return transactionId;
  }

  public synchronized LockType<K, V> getLock(K key) {
    return locksAcquired.get(key);
  }

  synchronized void putLock(K key, LockType<K, V> lockType) {
    locksAcquired.put(key, lockType);
  }

  /** Copy of locks_acquired, for commit/abort to release the locks one key latch at a time. */
  public synchronized Map<K, LockType<K, V>> locksSnapshot() {
    return new HashMap<>(locksAcquired);
  }

  public synchronized Status getStatus() {
    return status;
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

  public Timestamp getExpirationTime() {
    return expirationTime;
  }

  public void addToWriteSet(K key, V value) {
    writeSet.put(key, value);
  }

  public void addToReadSet(K key, V value) {
    readSet.put(key, value);
  }

  /**
   * CAS_returning of pseudo.txt: moves to {@code to} only if the current status is in {@code from}.
   * Returns the status before the call; the CAS succeeded iff {@code from} contains it.
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
