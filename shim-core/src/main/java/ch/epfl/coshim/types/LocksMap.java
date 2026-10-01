package ch.epfl.coshim.types;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * locks_map of pseudo.txt: one {@link Chain} per key. Chains are created with computeIfAbsent and
 * removed once empty; a request that fetched a chain that was removed meanwhile (retired) retries on
 * the key's current one, so a key never has two live chains.
 */
public class LocksMap<K, V> {
  private final Map<K, Chain<K, V>> locksMap;

  public LocksMap() {
    this.locksMap = new ConcurrentHashMap<>();
  }

  /** The key's current chain, if any (may be retired by the time its latch is taken). */
  public Chain<K, V> getChain(K key) {
    return locksMap.get(key);
  }

  /** See {@link Chain#acquire}: the node txn now holds on key, or null if txn is doomed. */
  public LockNode<K, V> acquire(K key, Transaction<K, V> txn, LockType.LockMode mode) {
    while (true) {
      Chain<K, V> chain = locksMap.computeIfAbsent(key, Chain::new);
      synchronized (chain) {
        if (!chain.isRetired()) {
          return chain.acquire(txn, mode);
        }
      }
    }
  }

  /**
   * See {@link Chain#upgrade}. txn holds a SHARED node on key, so the key's chain cannot be retired.
   */
  public Chain.UpgradeResult upgrade(K key, Transaction<K, V> txn) {
    Chain<K, V> chain = txn.getLock(key).node().getChain();
    synchronized (chain) {
      return chain.upgrade(txn);
    }
  }

  /**
   * Removes txn from node, splices node out if it is now empty, removes the chain if it is now empty,
   * and wakes the waiters. For abort_transaction, the cascade must have marked the successors (under
   * the same chain's latch) before this call.
   */
  public void release(LockNode<K, V> node, Transaction<K, V> txn) {
    Chain<K, V> chain = node.getChain();
    synchronized (chain) {
      node.removeHolder(txn);
      if (node.getHolders().isEmpty()) {
        chain.unlink(node);
      }
      if (chain.isEmpty()) {
        chain.retire();
        locksMap.remove(chain.getKey(), chain);
      }
      chain.signalAll();
    }
  }

  public boolean containsKey(K key) {
    return locksMap.containsKey(key);
  }
}
