package ch.epfl.coshim.types;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ch.epfl.coshim.types.LockType.LockMode;
import ch.epfl.coshim.types.LocksMap.LockResult;
import ch.epfl.coshim.types.LocksMap.ReadResult;
import ch.epfl.coshim.types.Transaction.Status;
import java.time.Duration;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import org.junit.jupiter.api.Test;

/**
 * Many txns on a few hot keys, with a "coordinator" aborting running txns from another thread, to
 * check that the per-key latching keeps the algorithm's requirements: no committed txn read from an
 * aborted one (cascade before splice), every lock is at the head of its chain when its txn commits
 * (CO), no node is leaked by a foreign abort, and the chains stay consistent throughout.
 *
 * <p>Each worker plays the shim (status CASes, buffers, store) the way pseudo.txt does. A written
 * value is the writer's id, so a read tells which txn it read from.
 */
class LocksMapStressTest {

  private static final List<String> KEYS = List.of("a", "b", "c");
  private static final int WORKERS = 8;
  private static final int TXNS_PER_WORKER = 400;
  private static final Duration LOCK_TIMEOUT = Duration.ofMillis(20);
  private static final Set<Status> ABORTABLE = EnumSet.of(Status.STARTED, Status.EXECUTED, Status.MUST_ABORT);

  private final LocksMap<String, String> locks = new LocksMap<>();
  private final Map<String, String> store = new ConcurrentHashMap<>();
  private final Map<String, Transaction<String, String>> txns = new ConcurrentHashMap<>();
  private final List<Transaction<String, String>> live = new CopyOnWriteArrayList<>();
  private final Map<Transaction<String, String>, Set<String>> readsFrom = new ConcurrentHashMap<>();
  private final Set<Transaction<String, String>> committed = ConcurrentHashMap.newKeySet();
  private final AtomicInteger aborted = new AtomicInteger();
  private final Queue<String> violations = new ConcurrentLinkedQueue<>();
  private final AtomicBoolean running = new AtomicBoolean(true);

  @Test
  void perKeyLatchingKeepsTheAlgorithmsInvariants() {
    assertTimeoutPreemptively(Duration.ofSeconds(120), this::run, "a wait never returned");

    assertTrue(violations.isEmpty(),() -> violations.size() + " violations, e.g. " + violations.peek());
    for (String k : KEYS) {
      assertTrue(!locks.containsKey(k), "leaked chain on " + k);
    }
    for (Transaction<String, String> t : txns.values()) {
      Status s = t.getStatus();
      assertTrue(s == Status.COMMITTED || s == Status.ABORTED, t.getTransactionId() + " ended " + s);
      if (s == Status.COMMITTED) {
        for (String writer : readsFrom.getOrDefault(t, Set.of())) {
          assertEquals(Status.COMMITTED, txns.get(writer).getStatus(),
              t.getTransactionId() + " committed after reading from " + writer);
        }
      }
    }
    assertEquals(WORKERS * TXNS_PER_WORKER, committed.size() + aborted.get());
    assertTrue(committed.size() > 0 && aborted.get() > 0, "the run exercised commits and aborts");
  }

  private void run() throws InterruptedException {
    List<Thread> workers = new ArrayList<>();
    for (int w = 0; w < WORKERS; w++) {
      int id = w;
      workers.add(new Thread(() -> worker(id)));
    }
    Thread coordinator = new Thread(this::coordinator);
    Thread checker = new Thread(this::checker);
    workers.forEach(Thread::start);
    coordinator.start();
    checker.start();
    for (Thread t : workers) {
      t.join();
    }
    running.set(false);
    coordinator.join();
    checker.join();
  }

  private void worker(int id) {
    Random rnd = new Random(id);
    for (int i = 0; i < TXNS_PER_WORKER; i++) {
      Transaction<String, String> t = new Transaction<>(id + "-" + i, Status.STARTED, null);
      txns.put(t.getTransactionId(), t);
      live.add(t);
      if (execute(t, rnd) && rnd.nextInt(10) != 0 && prepare(t)) {
        commit(t);
      } else {
        abortTransaction(t);
      }
      live.remove(t);
    }
  }

  /** execute(): 1 to 3 random get/put, then CAS started -> executed. */
  private boolean execute(Transaction<String, String> t, Random rnd) {
    int ops = 1 + rnd.nextInt(3);
    for (int op = 0; op < ops; op++) {
      String key = KEYS.get(rnd.nextInt(KEYS.size()));
      LockMode mode = rnd.nextBoolean() ? LockMode.SHARED : LockMode.EXCLUSIVE;
      LockResult r = locks.lock(key, t, mode, LOCK_TIMEOUT);
      if (r == LockResult.MUST_ABORT || r == LockResult.TIMED_OUT) {
        return false;
      }
      if (mode == LockMode.EXCLUSIVE) {
        t.addToWriteSet(key, t.getTransactionId());
      } else if (!t.getWriteSet().containsKey(key)) {
        ReadResult<String> read = locks.read(key, t);
        if (read.isDoomed()) {
          return false;
        }
        String value = read.value();
        if (value != null) {
          readsFrom.computeIfAbsent(t, x -> ConcurrentHashMap.newKeySet()).add(value);
        } else {
          value = store.get(key);
        }
        t.addToReadSet(key, value);
      }
    }
    if (!t.compareAndSwapStatus(Status.STARTED, Status.EXECUTED)) {
      return false;
    }
    locks.statusChanged(t);
    return true;
  }

  private boolean prepare(Transaction<String, String> t) {
    if (!locks.awaitPredecessorsResolved(t) || !t.compareAndSwapStatus(Status.EXECUTED, Status.PREPARED)) {
      return false;
    }
    locks.statusChanged(t);
    return true;
  }

  private void commit(Transaction<String, String> t) {
    if (!t.compareAndSwapStatus(Status.PREPARED, Status.COMMITTING)) {
      violations.add(t.getTransactionId() + " lost prepared before commit: " + t.getStatus());
      return;
    }
    locks.statusChanged(t);
    for (Map.Entry<String, LockType<String, String>> e : t.frozenLocks().entrySet()) {
      Chain<String, String> chain = locks.getChain(e.getKey());
      synchronized (chain) {
        if (chain.getHead() != e.getValue().node()) {
          violations.add(t.getTransactionId() + " commits but is not the head of " + e.getKey());
        }
      }
    }
    store.putAll(t.getWriteSet());
    locks.release(t);
    t.compareAndSwapStatus(Status.COMMITTING, Status.COMMITTED);
    committed.add(t);
  }

  /** abort_transaction(): idempotent; never takes a prepared txn (the coordinator never aborts it here). */
  private void abortTransaction(Transaction<String, String> t) {
    if (!ABORTABLE.contains(t.compareAndSwapStatus(ABORTABLE, Status.ABORTED))) {
      return;
    }
    aborted.incrementAndGet();
    for (Transaction<String, String> victim : locks.abortAndRelease(t)) {
      abortTransaction(victim);
    }
  }

  /** Aborts random live txns from a foreign thread, racing their own lock()/get()/prepare(). */
  private void coordinator() {
    Random rnd = new Random(-1);
    while (running.get()) {
      List<Transaction<String, String>> snapshot = new ArrayList<>(live);
      if (!snapshot.isEmpty() && rnd.nextInt(4) == 0) {
        abortTransaction(snapshot.get(rnd.nextInt(snapshot.size())));
      }
      LockSupport.parkNanos(100_000);
    }
  }

  /** Walks every chain under its latch: links agree both ways, lastNode is last, no empty node. */
  private void checker() {
    while (running.get()) {
      for (String k : KEYS) {
        Chain<String, String> chain = locks.getChain(k);
        if (chain == null) {
          continue;
        }
        synchronized (chain) {
          LockNode<String, String> prev = null;
          for (LockNode<String, String> n = chain.getHead(); n != null; n = n.getNext()) {
            if (n.getPrev() != prev) {
              violations.add("broken prev link on " + k);
            }
            if (n.getHolders().isEmpty()) {
              violations.add("empty node left in " + k);
            }
            if (n.getMode() == LockMode.EXCLUSIVE && n.getHolders().size() != 1) {
              violations.add("EXCLUSIVE node with " + n.getHolders().size() + " holders on " + k);
            }
            prev = n;
          }
          if (chain.getLastNode() != prev) {
            violations.add("lastNode is not the last node of " + k);
          }
        }
      }
    }
  }
}
