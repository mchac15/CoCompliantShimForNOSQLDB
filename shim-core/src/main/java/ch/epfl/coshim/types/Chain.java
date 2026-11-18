package ch.epfl.coshim.types;

import ch.epfl.coshim.types.LockType.LockMode;
import ch.epfl.coshim.types.Transaction.Status;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * The chain of one key (one per key, in {@link LocksMap}): generations of holders, doubly linked,
 * in the required commit order. The chain owns its {@link LockNode}s: every change to them goes
 * through the methods below.
 *
 * <p>Latching: {@link LocksMap} calls the methods that modify the chain ({@link #acquire}, {@link
 * #upgrade}, {@link #release}, {@link #abortAndRelease}) only inside {@code locksMap.compute(key,
 * ...)}, so they run under the key's map entry, and each of them is also {@code synchronized} on the
 * chain. Readers ({@link #awaitLockReady}, {@link #awaitResolved}, {@link #read}) only take the
 * chain's monitor, which is enough to see a consistent chain, and waiters {@code wait()} on it:
 * every change that can satisfy a wait predicate ends with {@code notifyAll()}. Lock order: map
 * entry, then chain, then txn.
 */
class Chain<K, V> {
  private static final Set<Status> MARKABLE = EnumSet.of(Status.STARTED, Status.EXECUTED);
  /** Holders already on their way out of the chain: they never stop a wound from unblocking. */
  private static final Set<Status> LEAVING = EnumSet.of(Status.MUST_ABORT, Status.ABORTED);
  /**
   * The predecessor holders prepare()'s wait-die never dies for: committed (the wait is over), committing
   * (decided, it commits without waiting for anyone), or leaving (it is released without waiting).
   */
  private static final Set<Status> PREPARE_NO_CYCLE =
      EnumSet.of(Status.COMMITTED, Status.COMMITTING, Status.MUST_ABORT, Status.ABORTED);

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
    TIMED_OUT,
    /**
     * Deadlock prevention: txn would wait for a younger txn, so it aborts instead. Detection: txn is
     * the youngest member of a wait-for cycle.
     */
    DIE,
    /**
     * Wound-wait: txn marked the younger running holders ahead of it must_abort (the victims).
     * Detection: the victim is the youngest member of a wait-for cycle through txn. The caller aborts
     * them outside the latch, then waits again.
     */
    WOUND
  }

  private final K key;
  private LockNode<K, V> head;
  /** The last node of the chain (pseudo.txt's chain.last_node()), kept to append in O(1). */
  private LockNode<K, V> lastNode;

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
   *
   * @param readsKey txn takes an EXCLUSIVE lock to read k before writing it (get-for-update). Its node is marked upgraded, as one upgrade() made: its
   *     holder read k, so it is not a blind write (the cascade must not stop at it, and an upgrade
   *     must not move another txn in front of it)
   * @return false if txn is no longer started (nothing was changed): it must abort
   */
  synchronized boolean acquire(Transaction<K, V> txn, LockMode mode, boolean readsKey) {
    synchronized (txn) {
      if (!txn.isRunning()) {
        return false;
      }
      LockNode<K, V> target;
      if (lastNode != null && mode == LockMode.SHARED && lastNode.getMode() == LockMode.SHARED) {
        lastNode.addHolder(txn);
        target = lastNode;
      } else {
        target = new LockNode<>(key, mode, txn);
        target.setUpgraded(readsKey && mode == LockMode.EXCLUSIVE);
        append(target);
      }
      txn.putLock(key, new LockType<>(mode, target));
      return true;
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
   * The wait loop of lock()/upgrade() (ready = at least executed): waits, at most until {@code
   * deadlineNanos}, until every holder of the predecessor of txn's node on this key is in {@code
   * ready}. The predecessor is re-read on every wake-up, since it is spliced out or replaced while
   * waiting. An aborted / doomed predecessor holder does not satisfy the wait: txn waits until it is
   * spliced out (the cascade marks the dependents before that).
   *
   * <p>Deadlock prevention, so that every wait-for edge goes from a younger txn to an older one and
   * no cycle can form. Chains are FIFO, so the common case (younger behind older) just waits.
   *
   * <ul>
   *   <li>wait-die: a txn that would wait for a younger one returns DIE instead.
   *   <li>wound-wait: the younger holders that are still started are marked must_abort here, in the
   *       same critical section as the age check (the point of no return: a victim can no longer
   *       reach executed, so nobody reads its buffer and it never votes). They are returned as
   *       {@code victims} with WOUND: aborting them takes map entries, so it must happen outside
   *       this monitor (lock order). When the caller comes back, the victims are aborted and wait
   *       like any other doomed holder until their release splices them out (with notifyAll), so
   *       nobody is wounded twice and the loop never spins. A younger holder that already executed
   *       does not block lock(), so it is never wounded; an older one is waited for.
   *       <p>Wounding happens only when it unblocks txn: while pred also holds a txn that cannot be
   *       wounded (an older running one, or in the non-speculative baseline an executed / prepared
   *       younger one), txn just waits and decides again on that txn's status change. The younger
   *       ones may execute meanwhile and then need not die. Still no cycle: txn only stays blocked
   *       on older txns or on txns past started (which wait on no lock), and as soon as those are
   *       gone it wounds the younger ones it would otherwise wait for.
   *   <li>detection: no age check; txn searches for a wait-for cycle through itself whenever it
   *       starts waiting behind a predecessor node it has not searched behind yet, and only a cycle
   *       aborts a txn (see {@link #detect}).
   * </ul>
   *
   * @param deadlineNanos a {@link System#nanoTime()} deadline
   * @param victims receives the txns marked on WOUND (only with WOUND_WAIT)
   */
  synchronized WaitResult awaitLockReady(
      Transaction<K, V> txn,
      Set<Status> ready,
      long deadlineNanos,
      DeadlockPolicy policy,
      List<Transaction<K, V>> victims) {
    txn.setWaitingOn(this);   // statistics only, see abortAndRelease
    txn.setBlockedOn(txn.getLock(key).node());   // before any check: see youngestOnCycle
    try {
      return awaitPredecessor(txn, ready, true, deadlineNanos, policy, victims);
    } finally {
      txn.setWaitingOn(null);
      txn.setBlockedOn(null);
    }
  }

  /**
   * The wait loop of prepare() (ready = committed): as {@link #awaitLockReady}, but unbounded and
   * with no deadlock prevention (it never dies nor wounds): its waits are left to the coordinator.
   * Returns READY or DOOMED (TIMED_OUT only if the thread is interrupted).
   */
  synchronized WaitResult awaitResolved(Transaction<K, V> txn, Set<Status> ready) {
    return awaitResolved(txn, ready, false);
  }

  /**
   * prepare()'s wait, with wait-die if {@code waitDie}: DIE as soon as a predecessor holder txn would
   * wait for is younger than txn (it may be executed or prepared, which nobody can wound, and its
   * commit may wait for txn's vote on another shim: the cross-shim cycle). Every wait of prepare()
   * then goes from a younger txn to an older one, on every shim alike since the ages are global, so
   * no cycle of prepare waits can form. Holders that commit or leave without waiting for anyone
   * (PREPARE_NO_CYCLE) are waited for. Re-checked on every wake-up: the predecessor changes as nodes
   * are spliced out, but nothing is ever put in front of a txn in prepare.
   */
  synchronized WaitResult awaitResolved(Transaction<K, V> txn, Set<Status> ready, boolean waitDie) {
    while (true) {
      if (txn.isDoomed()) {
        return WaitResult.DOOMED;
      }
      LockNode<K, V> pred = txn.getLock(key).node().getPrev();
      if (pred == null || pred.allHoldersIn(ready)) {
        return WaitResult.READY;
      }
      if (waitDie && pred.hasYoungerHolderNotIn(txn, PREPARE_NO_CYCLE)) {
        return WaitResult.DIE;
      }
      try {
        wait();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return WaitResult.TIMED_OUT;
      }
    }
  }

  private WaitResult awaitPredecessor(
      Transaction<K, V> txn,
      Set<Status> ready,
      boolean bounded,
      long deadlineNanos,
      DeadlockPolicy policy,
      List<Transaction<K, V>> victims) {
    LockNode<K, V> searched = null;   // detection: the predecessor txn last searched behind
    while (true) {
      if (txn.isDoomed()) {
        return WaitResult.DOOMED;
      }
      LockNode<K, V> pred = txn.getLock(key).node().getPrev();
      if (pred == null || pred.allHoldersIn(ready)) {
        if (bounded) {
          recordGrant(txn, pred);
        }
        return WaitResult.READY;
      }
      if (bounded && policy == DeadlockPolicy.WAIT_DIE && pred.hasYoungerHolderNotIn(txn, ready)) {
        return WaitResult.DIE;
      }
      if (bounded && policy == DeadlockPolicy.WOUND_WAIT && wound(txn, pred, ready, victims)) {
        return WaitResult.WOUND;
      }
      if (bounded && policy == DeadlockPolicy.DETECT) {
        WaitResult broken = detect(txn, pred, ready, pred != searched, victims);
        searched = pred;
        if (broken != null) {
          return broken;
        }
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
   * Wound-wait's marking, under this monitor: CASes every younger started holder of pred to
   * must_abort, but only when every holder of pred not in {@code ready} can be wounded or is already
   * leaving (must_abort / aborted): otherwise wounding would not unblock txn, which waits instead.
   * Also hands over, in any case, the holders that a cascade doomed while they were still running:
   * nobody else releases those until their own thread calls the shim again (e.g. during client think
   * time), and aborting them never cascades.
   *
   * <p>Statistics only: each victim is first checked for a wait-for path back to txn (a real
   * deadlock, which the wound breaks); see {@link Transaction#isWoundedInCycle}.
   *
   * @return true iff victims is non-empty: the caller must abort them before waiting again
   */
  private boolean wound(
      Transaction<K, V> txn,
      LockNode<K, V> pred,
      Set<Status> ready,
      List<Transaction<K, V>> victims) {
    boolean blocked = false;
    for (Transaction<K, V> t : pred.getHolders()) {
      Status s = t.getStatus();
      if (!ready.contains(s)
          && !LEAVING.contains(s)
          && !(s == Status.STARTED && t.getTs() > txn.getTs())) {
        blocked = true;   // an older live txn, or a younger one past started: not woundable
      }
    }
    for (Transaction<K, V> t : pred.getHolders()) {
      if (!blocked && t.getTs() > txn.getTs() && t.wound(reaches(t, txn, ready))) {
        victims.add(t);
      } else if (t.getStatus() == Status.MUST_ABORT && !t.hasReachedExecuted()) {
        victims.add(t);
      }
    }
    return !victims.isEmpty();
  }

  /**
   * Deadlock detection's step, under this monitor, before txn waits behind pred. If {@code search}
   * (pred is new to txn: it just blocked, or a node ahead was spliced out or put in front of it),
   * looks for a wait-for cycle through txn ({@link #youngestOnCycle}) and breaks it: txn dies if it
   * is the cycle's youngest member, else that member is wounded (it is started: blocked in
   * lock()/upgrade(), so it never executed and its abort cascades to nobody). A wound that fails
   * (the victim is no longer started) leaves a member already on its way out: txn waits. Searching
   * once per predecessor is enough: every new wait-for edge comes with a txn that starts waiting
   * behind a new predecessor (a txn blocking, a splice, or an upgrade that puts its node in front of
   * a blind writer and then waits itself), and that txn searches. Also hands over the holders a
   * cascade doomed while they were running, as {@link #wound} does.
   *
   * @return null to wait, DIE, or WOUND with the victims
   */
  private WaitResult detect(
      Transaction<K, V> txn,
      LockNode<K, V> pred,
      Set<Status> ready,
      boolean search,
      List<Transaction<K, V>> victims) {
    if (search) {
      Transaction<K, V> youngest = youngestOnCycle(txn, pred, ready);
      if (youngest == txn) {
        return WaitResult.DIE;
      }
      if (youngest != null && youngest.wound(true)) {
        victims.add(youngest);
      }
    }
    for (Transaction<K, V> t : pred.getHolders()) {
      if (t.getStatus() == Status.MUST_ABORT && !t.hasReachedExecuted() && !victims.contains(t)) {
        victims.add(t);
      }
    }
    return victims.isEmpty() ? null : WaitResult.WOUND;
  }

  /**
   * The wait-for graph's search, from txn, which waits for the holders of pred. A txn blocked behind
   * a node ({@link Transaction#getBlockedOn}) waits for every holder of that node's predecessor not
   * in {@code ready} and not leaving: those are its edges. Only this chain's monitor is held, so the
   * other chains are read without their latch (volatile prev, concurrent holder sets). The search may
   * then follow an edge that just disappeared (a phantom cycle: one needless abort, never a wrong
   * one), but it never misses a cycle that txn closes: every other member published its blockedOn
   * before its own search, and an edge of a cycle stays until a member aborts. Two members closing a
   * cycle at once see each other's blockedOn (volatile), so at least one of them finds the cycle, and
   * both pick the same victim.
   *
   * @return the youngest member (largest ts, then largest id) of a cycle through txn, null if none
   */
  private static <K, V> Transaction<K, V> youngestOnCycle(
      Transaction<K, V> txn, LockNode<K, V> pred, Set<Status> ready) {
    Map<Transaction<K, V>, Transaction<K, V>> parent = new HashMap<>();
    Transaction<K, V> last = pathTo(txn, waitedFor(pred, ready), ready, parent);
    if (last == null) {
      return null;
    }
    Transaction<K, V> youngest = txn;
    for (Transaction<K, V> t = last; t != txn; t = parent.get(t)) {
      if (t.getTs() > youngest.getTs()
          || t.getTs() == youngest.getTs() && t.getTransactionId().compareTo(youngest.getTransactionId()) > 0) {
        youngest = t;
      }
    }
    return youngest;
  }

  /** Statistics only: from, which txn waits for, waits (transitively) for txn: a wait-for cycle. */
  private static <K, V> boolean reaches(Transaction<K, V> from, Transaction<K, V> txn, Set<Status> ready) {
    return pathTo(txn, List.of(from), ready, new HashMap<>()) != null;
  }

  /**
   * Depth-first search of the wait-for graph (see {@link #youngestOnCycle}) from {@code starts} to
   * {@code target}.
   *
   * @param parent receives, for every txn reached, the one it was reached from ({@code target} for
   *     the starts): the path back from the result to target
   * @return the txn with an edge to target, null if target is unreachable
   */
  private static <K, V> Transaction<K, V> pathTo(
      Transaction<K, V> target,
      List<Transaction<K, V>> starts,
      Set<Status> ready,
      Map<Transaction<K, V>, Transaction<K, V>> parent) {
    Deque<Transaction<K, V>> stack = new ArrayDeque<>();
    for (Transaction<K, V> t : starts) {
      if (t != target && parent.putIfAbsent(t, target) == null) {
        stack.push(t);
      }
    }
    while (!stack.isEmpty()) {
      Transaction<K, V> u = stack.pop();
      LockNode<K, V> node = u.getBlockedOn();
      LockNode<K, V> pred = node == null ? null : node.getPrev();
      if (pred == null) {
        continue;   // u is running (or granted): it waits for nobody
      }
      for (Transaction<K, V> t : waitedFor(pred, ready)) {
        if (t == target) {
          return u;
        }
        if (parent.putIfAbsent(t, u) == null) {
          stack.push(t);
        }
      }
    }
    return null;
  }

  /** The holders of node a txn blocked behind it still waits for: not in ready, not leaving. */
  private static <K, V> List<Transaction<K, V>> waitedFor(LockNode<K, V> node, Set<Status> ready) {
    List<Transaction<K, V>> out = new ArrayList<>();
    for (Transaction<K, V> t : node.getHolders()) {
      Status s = t.getStatus();
      if (!ready.contains(s) && !LEAVING.contains(s)) {
        out.add(t);
      }
    }
    return out;
  }

  /** Statistics only: the uncommitted txns ahead of txn's node, and on how many nodes. */
  private void recordGrant(Transaction<K, V> txn, LockNode<K, V> pred) {
    int depth = 0;
    List<Transaction<K, V>> pending = new ArrayList<>();
    for (LockNode<K, V> n = pred; n != null; n = n.getPrev()) {
      boolean any = false;
      for (Transaction<K, V> t : n.getHolders()) {
        if (t.getStatus() != Status.COMMITTED) {
          pending.add(t);
          any = true;
        }
      }
      depth += any ? 1 : 0;
    }
    txn.recordGrant(depth, pending);
  }

  /**
   * get()'s critical section, for a txn that holds key: DOOMED if txn is aborted / must_abort (a
   * foreign abort_transaction may have spliced its node out since lock() returned); else the value
   * txn already read (read_buffer); else the write of the nearest EXCLUSIVE node before txn's own
   * node whose holder wrote key (txn's own node is never a predecessor version: if it is EXCLUSIVE,
   * txn holds it and its own write was already looked up by get(), e.g. a get-for-update before the
   * put; and a get-for-update holder that did not write key is skipped); else a null value (no predecessor version: read the store, a committed writer applied
   * its buffer before being spliced out). Reading that holder's buffer is safe: lock() only returned
   * once it was executed, so its buffer is final.
   */
  synchronized LocksMap.ReadResult<V> read(Transaction<K, V> txn) {
    if (txn.isDoomed()) {
      return LocksMap.ReadResult.doomed();
    }
    if (txn.getReadSet().containsKey(key)) {
      return LocksMap.ReadResult.of(txn.getReadSet().get(key));
    }
    // the nearest EXCLUSIVE node before txn's own one whose holder wrote key: an EXCLUSIVE node has
    // exactly one holder, and a get-for-update holder that never wrote key holds no version of it
    // (its successors read what it read). Holders ahead are executed: their buffers are final
    for (LockNode<K, V> n = txn.getLock(key).node().getPrev(); n != null; n = n.getPrev()) {
      if (n.getMode() == LockMode.EXCLUSIVE) {
        Map<K, V> writes = n.getHolders().iterator().next().getWriteSet();
        if (writes.containsKey(key)) {
          return LocksMap.ReadResult.of(writes.get(key));
        }
      }
    }
    return LocksMap.ReadResult.of(null);
  }

  /**
   * commit()'s release on this key: removes txn from node, splices node out if it is now empty.
   *
   * @return true iff the chain is now empty: the caller removes it from locks_map
   */
  synchronized boolean release(Transaction<K, V> txn, LockNode<K, V> node) {
    return releaseLocked(txn, node);
  }

  /**
   * abort_transaction()'s critical section on this key: if txn held it EXCLUSIVE, marks every holder
   * of the later nodes up to the next blind writer must_abort (mark_must_abort), and only then
   * releases (cascade before splice).
   *
   * <p>No cascade either if txn never reached executed (e.g. it was wounded, or aborted by the
   * coordinator while running): no successor got past its node, since none does before every holder
   * of its predecessor is at least executed, so none read its buffer. They only wait behind it and
   * are woken up by the release. Without this, an older txn waiting behind a younger writer would
   * doom itself by wounding it.
   *
   * <p>Without speculation there is nothing to cascade: every holder behind txn's uncommitted node
   * is still waiting in lock() (or holds a node behind such a waiter), so none has read txn's
   * buffer. They are only woken up by the release.
   *
   * @param cascade false for the non-speculative baseline: release only
   * @param marked receives every txn this call moved to must_abort (to wake them up)
   * @param doomedFinished receives those of them that were executed (the caller aborts them)
   * @param granted statistics only: for every holder met behind txn's node, whether it was met at
   *     least once on a node it had been granted (or upgraded to, having read the key): false means it
   *     was still blocked in lock() there, and had read nothing on that key
   * @return true iff the chain is now empty, as in {@link #release}
   */
  synchronized boolean abortAndRelease(
      Transaction<K, V> txn,
      LockType<K, V> held,
      boolean cascade,
      List<Transaction<K, V>> marked,
      List<Transaction<K, V>> doomedFinished,
      Map<Transaction<K, V>, Boolean> granted) {
    if (cascade && held.mode() == LockMode.EXCLUSIVE && txn.hasReachedExecuted()) {
      for (LockNode<K, V> s = held.node().getNext(); s != null; s = s.getNext()) {
        if (s.getMode() == LockMode.EXCLUSIVE && !s.isUpgraded()) {
          break;   // blind write: it does not depend on txn's value
        }
        for (Transaction<K, V> t : s.getHolders()) {
          granted.merge(t, s.isUpgraded() || !t.isWaitingOn(this), Boolean::logicalOr);
          Status before = t.markMustAbort(MARKABLE, txn.rootCause());
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
    return head == null;
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
