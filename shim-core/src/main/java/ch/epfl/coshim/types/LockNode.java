package ch.epfl.coshim.types;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * One generation of holders on a key. Every field is guarded by the latch of {@link #getChain()}:
 * read and write it only inside {@code synchronized (node.getChain())}.
 */
public class LockNode<K, V> {
  private final Chain<K, V> chain;
  private LockType.LockMode mode;
  private LockNode<K, V> next;
  private LockNode<K, V> prev;
  private final Set<Transaction<K, V>> holders = new HashSet<>();
  private boolean upgraded;

  LockNode(Chain<K, V> chain, LockType.LockMode mode, Transaction<K, V> holder) {
    this.chain = chain;
    this.mode = mode;
    this.holders.add(holder);
    this.upgraded = false;
  }

  public Chain<K, V> getChain() {
    return chain;
  }

  public K getKey() {
    return chain.getKey();
  }

  public LockNode<K, V> getNext() {
    return next;
  }

  void setNext(LockNode<K, V> next) {
    this.next = next;
  }

  public LockNode<K, V> getPrev() {
    return prev;
  }

  void setPrev(LockNode<K, V> prev) {
    this.prev = prev;
  }

  public LockType.LockMode getMode() {
    return mode;
  }

  void setMode(LockType.LockMode mode) {
    this.mode = mode;
  }

  /** Read-only view; iterate it under the chain latch only. */
  public Set<Transaction<K, V>> getHolders() {
    return Collections.unmodifiableSet(holders);
  }

  void addHolder(Transaction<K, V> transaction) {
    holders.add(transaction);
  }

  void removeHolder(Transaction<K, V> transaction) {
    holders.remove(transaction);
  }

  public boolean isUpgraded() {
    return upgraded;
  }

  void setUpgraded(boolean upgraded) {
    this.upgraded = upgraded;
  }
}
