package ch.epfl.coshim.types;

import ch.epfl.coshim.types.LockType.LockMode;
import ch.epfl.coshim.types.Transaction.Status;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * The chain of one key (one per key, in {@link LocksMap}): generations of holders, doubly linked,
 * in the required commit order. The chain owns its {@link LockNode}s: every change to them goes
 * through the methods below.
 *
 * <p>The chain object is the key's latch: each non-private method is {@code synchronized} and is one
 * whole critical section of pseudo.txt, so callers never take the latch themselves. Waiters {@code
 * wait()} on it, and every change that can satisfy a wait predicate ends with {@code notifyAll()}.
 */
class Chain<K, V> {
  private static final Set<Status> MARKABLE = EnumSet.of(Status.STARTED, Status.EXECUTED);

  static enum AcquireResult {
    ACQUIRED,
    /** txn is no longer started: it must not take new locks (doomed, or a put after end). */
    NOT_RUNNING,
    /** This chain was emptied and removed from locks_map: retry on the key's current chain. */
    RETIRED
  }

  static enum UpgradeResult {
    /** txn is no longer started, or read-modify-write against read-modify-write: txn aborts. */
    MUST_ABORT,
    /** Sole holder, flipped in place: nothing new to wait for. */
    UPGRADED,
    /** Moved to a new EXCLUSIVE node behind readers: wait for its predecessor. */
    UPGRADED_MUST_WAIT
  }

  static enum WaitResult {
    READY,
    /** txn is aborted or must_abort. */
    DOOMED,
    /** The deadline passed (or the thread was interrupted). */
    TIMED_OUT
  }

  private final K key;
  private LockNode<K, V> head;
  /** The last node of the chain (pseudo.txt's chain.last_node()), kept to append in O(1). */
  private LockNode<K, V> lastNode;
  private boolean retired;

  Chain(K key) {
    this.key = key;
  }

  synchronized LockNode<K, V> getHead() {
    return head;
  }

  synchronized LockNode<K, V> getLastNode() {
    return lastNode;
  }

  synchronized boolean isEmpty() {
    return head == null;
  }

  synchronized void signalAll() {
    notifyAll();
  }

  /**
   * The critical section of lock(k, mode, txn): re-checks that txn is still running (a foreign
   * abort_transaction may have released its locks meanwhile, or end() ran), joins the last node if
   * both are SHARED or appends a new node, and records it in txn.locks_acquired.
   */
  synchronized AcquireResult acquire(Transaction<K, V> txn, LockMode mode) {
    if (retired) {
      return AcquireResult.RETIRED;
    }
    synchronized (txn) {
      if (!txn.isRunning()) {
        return AcquireResult.NOT_RUNNING;
      }
      LockNode<K, V> target;
      if (lastNode != null && mode == LockMode.SHARED && lastNode.getMode() == LockMode.SHARED) {
        lastNode.addHolder(txn);
        target = lastNode;
      } else {
        target = new LockNode<>(key, mode, txn);
        append(target);
      }
      txn.putLock(key, new LockType<>(mode, target));
      return AcquireResult.ACQUIRED;
    }
  }

  /**
   * The critical section of upgrade(k, txn): SHARED to EXCLUSIVE, moving txn only across readers,
   * never across a writer. txn.getLock(key) holds the resulting node afterwards.
   */
  synchronized UpgradeResult upgrade(Transaction<K, V> txn) {
    synchronized (txn) {
      if (!txn.isRunning()) {
        return UpgradeResult.MUST_ABORT;
      }
      LockNode<K, V> node = txn.getLock(key).node();

      // skip the readers behind txn's own node (not the chain's head): only those may be crossed
      LockNode<K, V> p = node;
      while (p.getNext() != null && p.getNext().getMode() == LockMode.SHARED) {
        p = p.getNext();
      }
      LockNode<K, V> after = p.getNext();   // EXCLUSIVE or null

      if (after != null && after.isUpgraded()) {
        return UpgradeResult.MUST_ABORT;   // case 3b: RMW vs RMW, this one loses
      }
      if (p == node && node.getHolders().size() == 1) {
        // case 1: sole holder, at most a blind writer behind: upgrade in place
        node.setMode(LockMode.EXCLUSIVE);
        node.setUpgraded(true);
        txn.putLock(key, new LockType<>(LockMode.EXCLUSIVE, node));
        return UpgradeResult.UPGRADED;
      }
      // cases 2 and 3a: a new EXCLUSIVE node right behind p (at the end, or in front of the blind
      // writer `after`)
      node.removeHolder(txn);
      LockNode<K, V> newNode = new LockNode<>(key, LockMode.EXCLUSIVE, txn);
      newNode.setUpgraded(true);
      insertAfter(p, newNode);
      if (node.getHolders().isEmpty()) {   // only possible when p != node
        unlink(node);
      }
      txn.putLock(key, new LockType<>(LockMode.EXCLUSIVE, newNode));
      notifyAll();   // node lost a holder, and `after` has a new predecessor
      return UpgradeResult.UPGRADED_MUST_WAIT;
    }
  }

  /**
   * The wait loop of lock()/upgrade() (ready = at least executed, bounded) and of prepare() (ready =
   * committed, unbounded): waits until every holder of the predecessor of txn's node on this key is
   * in {@code ready}. The predecessor is re-read on every wake-up, since it is spliced out or
   * replaced while waiting. An aborted / doomed predecessor holder does not satisfy the wait: txn
   * waits until it is spliced out (the cascade marks the dependents before that).
   *
   * @param deadlineNanos a {@link System#nanoTime()} deadline, used only if {@code bounded}
   */
  synchronized WaitResult awaitPredecessor(
      Transaction<K, V> txn, Set<Status> ready, boolean bounded, long deadlineNanos) {
    while (true) {
      if (txn.isDoomed()) {
        return WaitResult.DOOMED;
      }
      LockNode<K, V> pred = txn.getLock(key).node().getPrev();
      if (pred == null || pred.allHoldersIn(ready)) {
        return WaitResult.READY;
      }
      try {
        if (bounded) {
          long remaining = deadlineNanos - System.nanoTime();
          if (remaining <= 0) {
            return WaitResult.TIMED_OUT;
          }
          TimeUnit.NANOSECONDS.timedWait(this, remaining);
        } else {
          wait();
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return WaitResult.TIMED_OUT;
      }
    }
  }

  /**
   * commit()'s release on this key: removes txn from node, splices node out if it is now empty.
   *
   * @return true iff the chain is now empty and retired: the caller removes it from locks_map
   */
  synchronized boolean release(Transaction<K, V> txn, LockNode<K, V> node) {
    return releaseLocked(txn, node);
  }

  /**
   * abort_transaction()'s critical section on this key: if txn held it EXCLUSIVE, marks every holder
   * of the later nodes up to the next blind writer must_abort (mark_must_abort), and only then
   * releases (cascade before splice).
   *
   * @param marked receives every txn this call moved to must_abort (to wake them up)
   * @param doomedFinished receives those of them that were executed (the caller aborts them)
   * @return true iff the chain is now empty and retired, as in {@link #release}
   */
  synchronized boolean abortAndRelease(
      Transaction<K, V> txn,
      LockType<K, V> held,
      List<Transaction<K, V>> marked,
      List<Transaction<K, V>> doomedFinished) {
    if (held.mode() == LockMode.EXCLUSIVE) {
      for (LockNode<K, V> s = held.node().getNext(); s != null; s = s.getNext()) {
        if (s.getMode() == LockMode.EXCLUSIVE && !s.isUpgraded()) {
          break;   // blind write: it does not depend on txn's value
        }
        for (Transaction<K, V> t : s.getHolders()) {
          Status before = t.compareAndSwapStatus(MARKABLE, Status.MUST_ABORT);
          if (MARKABLE.contains(before)) {
            marked.add(t);
            if (before == Status.EXECUTED) {
              doomedFinished.add(t);
            }
          }
        }
      }
    }
    return releaseLocked(txn, held.node());
  }

  private boolean releaseLocked(Transaction<K, V> txn, LockNode<K, V> node) {
    node.removeHolder(txn);
    if (node.getHolders().isEmpty()) {
      unlink(node);
    }
    notifyAll();
    if (head == null) {
      retired = true;
    }
    return retired;
  }

  /** "append n": links n behind the last node; n becomes the last node (and the head if empty). */
  private void append(LockNode<K, V> node) {
    if (lastNode == null) {
      head = node;   // the chain was empty: node is both its first and its last node
      lastNode = node;
    } else {
      insertAfter(lastNode, node);
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
      lastNode = node;
    }
  }

  /** Splices node out (by identity), relinking both neighbours and fixing head/lastNode. */
  private void unlink(LockNode<K, V> node) {
    if (node != head && node.getPrev() == null) {
      return;   // not in the chain (already spliced out): `prev == null` would wrongly mean "head"
    }
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
      lastNode = before;
    }
    node.setPrev(null);
    node.setNext(null);
  }
}
