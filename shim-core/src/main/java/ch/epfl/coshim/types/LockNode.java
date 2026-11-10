package ch.epfl.coshim.types;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * One generation of holders on a key. A passive record: only the key's {@link Chain} reads or
 * modifies it, under its latch.
 */
public class LockNode<K, V> {
  private final K key;
  private LockType.LockMode mode;
  private LockNode<K, V> next;
  private LockNode<K, V> prev;
  private final Set<Transaction<K, V>> holders = new HashSet<>();
  /**
   * The holders that may have read the key through this node: granted here (lock() got past its
   * wait), or moved here by an upgrade after reading it as a reader. The others are still blocked in
   * lock() and read nothing of the txns ahead: a cascade spares them.
   */
  private final Set<Transaction<K, V>> granted = new HashSet<>();
  private boolean upgraded;

  LockNode(K key, LockType.LockMode mode, Transaction<K, V> holder) {
    this.key = key;
    this.mode = mode;
    this.holders.add(holder);
    this.upgraded = false;
  }

  K getKey() {
    return key;
  }

  LockNode<K, V> getNext() {
    return next;
  }

  void setNext(LockNode<K, V> next) {
    this.next = next;
  }

  LockNode<K, V> getPrev() {
    return prev;
  }

  void setPrev(LockNode<K, V> prev) {
    this.prev = prev;
  }

  LockType.LockMode getMode() {
    return mode;
  }

  void setMode(LockType.LockMode mode) {
    this.mode = mode;
  }

  Set<Transaction<K, V>> getHolders() {
    return Collections.unmodifiableSet(holders);
  }

  void addHolder(Transaction<K, V> transaction) {
    holders.add(transaction);
  }

  void removeHolder(Transaction<K, V> transaction) {
    holders.remove(transaction);
    granted.remove(transaction);
  }

  void markGranted(Transaction<K, V> transaction) {
    granted.add(transaction);
  }

  /** The holder may have read the key through this node (see {@code granted}). */
  boolean isGranted(Transaction<K, V> transaction) {
    return granted.contains(transaction);
  }

  /** ∀ t in holders, t.status ∈ statuses. */
  boolean allHoldersIn(Set<Transaction.Status> statuses) {
    for (Transaction<K, V> t : holders) {
      if (!statuses.contains(t.getStatus())) {
        return false;
      }
    }
    return true;
  }

  /**
   * ∃ t in holders, t.status ∉ statuses ∧ t is younger than txn: txn would wait for a younger txn.
   * Holders already in {@code statuses} are not waited for, so they do not count.
   */
  boolean hasYoungerHolderNotIn(Transaction<K, V> txn, Set<Transaction.Status> statuses) {
    for (Transaction<K, V> t : holders) {
      if (t.isYoungerThan(txn) && !statuses.contains(t.getStatus())) {
        return true;
      }
    }
    return false;
  }

  boolean isUpgraded() {
    return upgraded;
  }

  void setUpgraded(boolean upgraded) {
    this.upgraded = upgraded;
  }
}
