package ch.epfl.coshim.types;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * One generation of holders on a key. A passive record: only the key's {@link Chain} modifies it,
 * under its latch. The deadlock detection ({@link DeadlockPolicy#DETECT}) also reads {@code prev} and
 * the holders of nodes of other chains without their latch (it may hold one chain's monitor only),
 * hence the volatile pointer and the concurrent set.
 */
public class LockNode<K, V> {
  private final K key;
  private LockType.LockMode mode;
  private LockNode<K, V> next;
  private volatile LockNode<K, V> prev;
  private final Set<Transaction<K, V>> holders = ConcurrentHashMap.newKeySet();
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
      if (t.getTs() > txn.getTs() && !statuses.contains(t.getStatus())) {
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
