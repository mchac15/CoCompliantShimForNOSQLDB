package ch.epfl.coshim.types;

import java.util.HashSet;
import java.util.Set;

public class LockNode<K, V> {
  private final K key;
  private LockType.LockMode mode;
  private LockNode<K, V> next;
  private LockNode<K, V> prev;
  private Set<Transaction<K, V>> transactionIds;
  private boolean upgraded;

  public LockNode(K key, LockType.LockMode mode) {
    this.key = key;
    this.mode = mode;
    this.transactionIds = new HashSet<>();
    this.upgraded = false;
  }

  public K getKey() {
    return key;
  }

  public LockNode<K, V> getNext() {
    return next;
  }

  public void setNext(LockNode<K, V> next) {
    this.next = next;
  }

  public LockNode<K, V> getPrev() {
    return prev;
  }

  public void setPrev(LockNode<K, V> prev) {
    this.prev = prev;
  }

  public LockType.LockMode getMode() {
    return mode;
  }

  public Set<Transaction<K, V>> getTransactionIds() {
    return transactionIds;
  }

  public void addTransactionId(Transaction<K, V> transaction) {
    this.transactionIds.add(transaction);
  }

  public void removeTransactionId(Transaction<K, V> transaction) {
    this.transactionIds.remove(transaction);
  }

  public boolean isUpgraded() {
    return upgraded;
  }

  public void setUpgraded(boolean upgraded) {
    this.upgraded = upgraded;
  }

  public void setMode(LockType.LockMode mode) {
    this.mode = mode;
  }
}
