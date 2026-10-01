package ch.epfl.coshim.types;

import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;

public class Transaction<K, V> {
  private final String transactionId;
  private final Map<K, LockType<K>> locksAcquired;
  private final Map<K, V> writeSet;
  private final Map<K, V> readSet;
  private final Timestamp expirationTime;
  private final Object statusLock = new Object();
  private Status status;

  Transaction(String transactionId, Status status, Timestamp expirationTime) {
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

  public Map<K, LockType<K>> getLocksAcquired() {
    return locksAcquired;
  }

  public Status getStatus() {
    synchronized (statusLock) {
      return status;
    }
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

  public void addLock(K key, LockType<K> lockType) {
    locksAcquired.put(key, lockType);
  }

  public void addToWriteSet(K key, V value) {
    writeSet.put(key, value);
  }

  public void addToReadSet(K key, V value) {
    readSet.put(key, value);
  }

  public Status compareAndSwapStatus(Status status) {
    Status oldStatus;
    synchronized (statusLock) {
      oldStatus = this.status;
      this.status = status;
    }
    return oldStatus;
  }

  private void setStatus(Status status) {
    synchronized (statusLock) {
      this.status = status;
    }
  }

  static enum Status {
    STARTED,
    EXECUTED,
    PREPARED,
    COMMITTED,
    ABORTED,
    COMMITTING,
    MUST_ABORT
  }
}
