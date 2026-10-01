package ch.epfl.coshim.types;

/**
 * The chain of one key: generations of holders, doubly linked, in the required commit order.
 *
 * <p>The chain object itself is the key's latch: every method requires the caller to hold {@code
 * synchronized (chain)}, and waiters in lock()/upgrade() {@code wait()} on it. Whoever changes a
 * holder's status or the chain's links calls {@link #signalAll()} so that they re-read their
 * predecessor.
 */
public class Chain<K, V> {
  private final K key;
  private LockNode<K, V> head;
  private LockNode<K, V> tail;
  private boolean retired;

  Chain(K key) {
    this.key = key;
  }

  public K getKey() {
    return key;
  }

  public LockNode<K, V> getHead() {
    assert Thread.holdsLock(this);
    return head;
  }

  public LockNode<K, V> lastNode() {
    assert Thread.holdsLock(this);
    return tail;
  }

  public boolean isEmpty() {
    assert Thread.holdsLock(this);
    return head == null;
  }

  /** Removed from {@link LocksMap}: a new request must fetch the key's current chain instead. */
  boolean isRetired() {
    return retired;
  }

  void retire() {
    retired = true;
  }

  public void signalAll() {
    assert Thread.holdsLock(this);
    notifyAll();
  }

  /** "append n": links n behind the tail; n becomes the tail (and the head if the chain was empty). */
  private void append(LockNode<K, V> node) {
    if (tail == null) {
      head = node;
      tail = node;
    } else {
      insertAfter(tail, node);
    }
  }

  /** Links node right behind p, relinking both of its neighbours. */
  private void insertAfter(LockNode<K, V> p, LockNode<K, V> node) {
    LockNode<K, V> after = p.getNext();
    node.setPrev(p);
    node.setNext(after);
    p.setNext(node);
    if (after != null) {
      after.setPrev(node);   // its wait loop re-reads predecessor, so it now also waits for node
    } else {
      tail = node;
    }
  }

  /** Splices node out (by identity), relinking both neighbours and fixing head/tail. */
  void unlink(LockNode<K, V> node) {
    assert Thread.holdsLock(this);
    LockNode<K, V> before = node.getPrev();
    LockNode<K, V> after = node.getNext();
    if (before != null) {
      before.setNext(after);
    } else {
      head = after;
    }
    if (after != null) {
      after.setPrev(before);
    } else {
      tail = before;
    }
    node.setPrev(null);
    node.setNext(null);
  }

  /**
   * The critical section of lock(k, mode, txn): re-checks under the latch that txn is not doomed
   * (a foreign abort_transaction may have released its locks meanwhile), joins the tail if both are
   * SHARED or appends a new node, and records it in txn.locks_acquired. The ALREADY_HELD / upgrade
   * dispatch happens before, in the caller.
   *
   * @return the node txn now holds, or null if txn is aborted or must_abort (MUST_ABORT)
   */
  LockNode<K, V> acquire(Transaction<K, V> txn, LockType.LockMode mode) {
    assert Thread.holdsLock(this);
    synchronized (txn) {
      if (txn.isDoomed()) {
        return null;
      }
      LockNode<K, V> target;
      if (tail != null && mode == LockType.LockMode.SHARED && tail.getMode() == LockType.LockMode.SHARED) {
        tail.addHolder(txn);
        target = tail;
      } else {
        target = new LockNode<>(this, mode, txn);
        append(target);
      }
      txn.putLock(key, new LockType<>(mode, target));
      return target;
    }
  }

  public enum UpgradeResult {
    /** txn is doomed, or read-modify-write against read-modify-write: txn must abort. */
    MUST_ABORT,
    /** Sole holder, flipped in place: nothing new to wait for. */
    UPGRADED,
    /** Moved to a new EXCLUSIVE node behind readers: wait for its predecessor like in lock(). */
    UPGRADED_MUST_WAIT
  }

  /**
   * The critical section of upgrade(k, txn): SHARED to EXCLUSIVE, moving txn only across readers,
   * never across a writer. The new node (if any) is in txn.getLock(key).node() afterwards.
   */
  UpgradeResult upgrade(Transaction<K, V> txn) {
    assert Thread.holdsLock(this);
    synchronized (txn) {
      if (txn.isDoomed()) {
        return UpgradeResult.MUST_ABORT;
      }
      LockNode<K, V> node = txn.getLock(key).node();

      // skip the readers behind txn's own node (not the chain's head): only those may be crossed
      LockNode<K, V> p = node;
      while (p.getNext() != null && p.getNext().getMode() == LockType.LockMode.SHARED) {
        p = p.getNext();
      }
      LockNode<K, V> after = p.getNext();   // EXCLUSIVE or null

      if (after != null && after.isUpgraded()) {
        return UpgradeResult.MUST_ABORT;   // case 3b: RMW vs RMW, this one loses
      }
      if (p == node && node.getHolders().size() == 1) {
        // case 1: sole holder, at most a blind writer behind: upgrade in place
        node.setMode(LockType.LockMode.EXCLUSIVE);
        node.setUpgraded(true);
        txn.putLock(key, new LockType<>(LockType.LockMode.EXCLUSIVE, node));
        return UpgradeResult.UPGRADED;
      }
      // cases 2 and 3a: a new EXCLUSIVE node right behind p (at the tail, or in front of the blind
      // writer `after`)
      node.removeHolder(txn);
      LockNode<K, V> newNode = new LockNode<>(this, LockType.LockMode.EXCLUSIVE, txn);
      newNode.setUpgraded(true);
      insertAfter(p, newNode);
      if (node.getHolders().isEmpty()) {   // only possible when p != node
        unlink(node);
      }
      txn.putLock(key, new LockType<>(LockType.LockMode.EXCLUSIVE, newNode));
      signalAll();   // predecessors changed for the nodes behind
      return UpgradeResult.UPGRADED_MUST_WAIT;
    }
  }
}
