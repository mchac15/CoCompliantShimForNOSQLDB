package ch.epfl.coshim.types;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ch.epfl.coshim.types.LockType.LockMode;
import ch.epfl.coshim.types.LocksMap.LockResult;
import ch.epfl.coshim.types.Transaction.Status;
import java.time.Duration;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.junit.jupiter.api.Test;

class LocksMapTest {

  private static final Duration LONG = Duration.ofSeconds(5);

  private final LocksMap<String, String> locks = new LocksMap<>();

  private static Transaction<String, String> txn(String id) {
    return new Transaction<>(id, Status.STARTED, null);
  }

  private void enqueue(Transaction<String, String> t, LockMode mode) {
    assertNotNull(locks.enqueue("x", t, mode));
  }

  /** Moves t to status and wakes its waiters, like the shim does after every CAS. */
  private void set(Transaction<String, String> t, Status status) {
    t.compareAndSwapStatus(EnumSet.allOf(Status.class), status);
    locks.statusChanged(t);
  }

  /** The holders of each node, head to last, walking next; checks prev/head/lastNode agree. */
  private List<Set<Transaction<String, String>>> chain() {
    Chain<String, String> chain = locks.getChain("x");
    List<Set<Transaction<String, String>>> out = new ArrayList<>();
    if (chain == null) {
      return out;
    }
    synchronized (chain) {
      LockNode<String, String> prev = null;
      for (LockNode<String, String> n = chain.getHead(); n != null; n = n.getNext()) {
        assertSame(prev, n.getPrev(), "prev pointer disagrees with next");
        out.add(Set.copyOf(n.getHolders()));
        prev = n;
      }
      assertSame(prev, chain.getLastNode(), "lastNode is not the last node");
    }
    return out;
  }

  private Chain.UpgradeResult upgrade(Transaction<String, String> t) {
    return locks.getChain("x").upgrade(t);
  }

  private static <T> void assertBlocked(CompletableFuture<T> f) {
    assertThrows(TimeoutException.class, () -> f.get(100, TimeUnit.MILLISECONDS));
  }

  // ---- chain structure ----

  @Test
  void releasingTheLastNodeRemovesTheChainAndANewRequestGetsAFreshOne() {
    Transaction<String, String> a = txn("a");
    enqueue(a, LockMode.EXCLUSIVE);
    Chain<String, String> old = locks.getChain("x");

    set(a, Status.COMMITTING);
    locks.release(a);

    assertFalse(locks.containsKey("x"));
    Transaction<String, String> b = txn("b");
    enqueue(b, LockMode.SHARED);
    assertTrue(locks.getChain("x") != old);
    assertEquals(List.of(Set.of(b)), chain());
  }

  @Test
  void releasingANonHeadNodeKeepsTheHeadAndASecondReleaseIsANoOp() {
    Transaction<String, String> a = txn("a");
    Transaction<String, String> b = txn("b");
    Transaction<String, String> c = txn("c");
    enqueue(a, LockMode.EXCLUSIVE);
    enqueue(b, LockMode.EXCLUSIVE);
    enqueue(c, LockMode.EXCLUSIVE);

    set(b, Status.COMMITTING);
    locks.release(b);
    locks.release(b);

    assertEquals(List.of(Set.of(a), Set.of(c)), chain());
  }

  @Test
  void onlyARunningTxnTakesLocks() {
    Transaction<String, String> doomed = txn("d");
    set(doomed, Status.MUST_ABORT);
    Transaction<String, String> ended = txn("e");
    set(ended, Status.EXECUTED);

    assertEquals(LockResult.MUST_ABORT, locks.lock("x", doomed, LockMode.SHARED, LONG));
    assertEquals(LockResult.MUST_ABORT, locks.lock("x", ended, LockMode.SHARED, LONG));
    assertNull(doomed.getLock("x"));
    assertNull(ended.getLock("x"));
  }

  @Test
  void frozenLocksRefusesARunningTxn() {
    assertThrows(IllegalStateException.class, () -> locks.release(txn("a")));
  }

  @Test
  void upgradeInPlaceWhenSoleHolder() {
    Transaction<String, String> t = txn("t");
    assertEquals(LockResult.ACQUIRED, locks.lock("x", t, LockMode.SHARED, LONG));
    LockNode<String, String> node = t.getLock("x").node();

    assertEquals(LockResult.ACQUIRED, locks.lock("x", t, LockMode.EXCLUSIVE, LONG));

    assertSame(node, t.getLock("x").node());
    assertEquals(LockMode.EXCLUSIVE, t.getLock("x").mode());
    assertTrue(node.isUpgraded());
    assertEquals(LockResult.ALREADY_HELD, locks.lock("x", t, LockMode.SHARED, LONG));
  }

  @Test
  void upgradeScansFromItsOwnNodeNotTheHead() {
    // S(a) <- X(b, blind) <- S(t, c): t must stay behind b, it read b's value
    Transaction<String, String> a = txn("a");
    Transaction<String, String> b = txn("b");
    Transaction<String, String> t = txn("t");
    Transaction<String, String> c = txn("c");
    enqueue(a, LockMode.SHARED);
    enqueue(b, LockMode.EXCLUSIVE);
    enqueue(t, LockMode.SHARED);
    enqueue(c, LockMode.SHARED);

    assertEquals(Chain.UpgradeResult.UPGRADED_MUST_WAIT, upgrade(t));

    assertEquals(List.of(Set.of(a), Set.of(b), Set.of(c), Set.of(t)), chain());
    assertEquals(LockMode.EXCLUSIVE, t.getLock("x").mode());
    assertTrue(t.getLock("x").node().isUpgraded());
  }

  @Test
  void upgradeInFrontOfABlindWriterIsLinkedForward() {
    // S(t, r) <- X(w, blind): t goes between its readers' node and w
    Transaction<String, String> t = txn("t");
    Transaction<String, String> r = txn("r");
    Transaction<String, String> w = txn("w");
    enqueue(t, LockMode.SHARED);
    enqueue(r, LockMode.SHARED);
    enqueue(w, LockMode.EXCLUSIVE);

    assertEquals(Chain.UpgradeResult.UPGRADED_MUST_WAIT, upgrade(t));

    assertEquals(List.of(Set.of(r), Set.of(t), Set.of(w)), chain());
    synchronized (locks.getChain("x")) {
      assertSame(t.getLock("x").node(), w.getLock("x").node().getPrev());
    }
  }

  @Test
  void upgradeSplicesOutItsOwnEmptyNodeOnlyAndKeepsTheHead() {
    // X(h) <- S(t) <- X(w) <- S(r), then w aborts: X(h) <- S(t) <- S(r)
    Transaction<String, String> h = txn("h");
    Transaction<String, String> t = txn("t");
    Transaction<String, String> w = txn("w");
    Transaction<String, String> r = txn("r");
    enqueue(h, LockMode.EXCLUSIVE);
    enqueue(t, LockMode.SHARED);
    enqueue(w, LockMode.EXCLUSIVE);
    enqueue(r, LockMode.SHARED);
    set(w, Status.ABORTED);
    locks.abortAndRelease(w);   // r is behind w and gets marked; t is ahead of it
    assertEquals(Status.STARTED, t.getStatus());

    assertEquals(Chain.UpgradeResult.UPGRADED_MUST_WAIT, upgrade(t));

    assertEquals(List.of(Set.of(h), Set.of(r), Set.of(t)), chain());
  }

  @Test
  void upgradeBehindAnotherUpgraderAborts() {
    // S(t, u) where u upgraded first: S(t) <- X(u, upgraded); t loses
    Transaction<String, String> t = txn("t");
    Transaction<String, String> u = txn("u");
    enqueue(t, LockMode.SHARED);
    enqueue(u, LockMode.SHARED);
    assertEquals(Chain.UpgradeResult.UPGRADED_MUST_WAIT, upgrade(u));

    assertEquals(LockResult.MUST_ABORT, locks.lock("x", t, LockMode.EXCLUSIVE, LONG));
    assertEquals(LockMode.SHARED, t.getLock("x").mode());
  }

  // ---- waits, cascade, wake-ups ----

  @Test
  void lockWaitsUntilThePredecessorExecuted() throws Exception {
    Transaction<String, String> a = txn("a");
    Transaction<String, String> b = txn("b");
    assertEquals(LockResult.ACQUIRED, locks.lock("x", a, LockMode.EXCLUSIVE, LONG));

    CompletableFuture<LockResult> f =
        CompletableFuture.supplyAsync(() -> locks.lock("x", b, LockMode.SHARED, LONG));
    assertBlocked(f);

    set(a, Status.EXECUTED);
    assertEquals(LockResult.ACQUIRED, f.get(2, TimeUnit.SECONDS));
  }

  @Test
  void lockTimesOutBehindARunningPredecessor() {
    Transaction<String, String> a = txn("a");
    Transaction<String, String> b = txn("b");
    locks.lock("x", a, LockMode.EXCLUSIVE, LONG);

    assertEquals(LockResult.TIMED_OUT, locks.lock("x", b, LockMode.SHARED, Duration.ofMillis(50)));
  }

  @Test
  void aWaiterMarkedByACascadeWakesUpAndAborts() throws Exception {
    Transaction<String, String> a = txn("a");
    Transaction<String, String> b = txn("b");
    locks.lock("x", a, LockMode.EXCLUSIVE, LONG);
    CompletableFuture<LockResult> f =
        CompletableFuture.supplyAsync(() -> locks.lock("x", b, LockMode.SHARED, LONG));
    assertBlocked(f);

    set(a, Status.ABORTED);
    List<Transaction<String, String>> finished = locks.abortAndRelease(a);

    assertEquals(LockResult.MUST_ABORT, f.get(2, TimeUnit.SECONDS));
    assertEquals(Status.MUST_ABORT, b.getStatus());
    assertTrue(finished.isEmpty(), "b was still running: its own thread aborts it");
  }

  // ---- non-speculative baseline ----

  @Test
  void nonSpeculativeLockWaitsForThePredecessorToCommitNotJustToExecute() throws Exception {
    LocksMap<String, String> s2pl = new LocksMap<>(false);
    Transaction<String, String> a = txn("a");
    Transaction<String, String> b = txn("b");
    assertEquals(LockResult.ACQUIRED, s2pl.lock("x", a, LockMode.EXCLUSIVE, LONG));
    CompletableFuture<LockResult> f =
        CompletableFuture.supplyAsync(() -> s2pl.lock("x", b, LockMode.SHARED, LONG));

    a.compareAndSwapStatus(EnumSet.allOf(Status.class), Status.EXECUTED);
    s2pl.statusChanged(a);
    assertBlocked(f);
    a.compareAndSwapStatus(EnumSet.allOf(Status.class), Status.PREPARED);
    s2pl.statusChanged(a);
    assertBlocked(f);

    a.compareAndSwapStatus(EnumSet.allOf(Status.class), Status.COMMITTING);
    s2pl.release(a);
    assertEquals(LockResult.ACQUIRED, f.get(2, TimeUnit.SECONDS));
  }

  @Test
  void nonSpeculativeAbortReleasesTheWaitersWithoutMarkingThem() throws Exception {
    LocksMap<String, String> s2pl = new LocksMap<>(false);
    Transaction<String, String> a = txn("a");
    Transaction<String, String> b = txn("b");
    Transaction<String, String> c = txn("c");
    s2pl.lock("x", a, LockMode.EXCLUSIVE, LONG);
    a.compareAndSwapStatus(EnumSet.allOf(Status.class), Status.EXECUTED);
    s2pl.statusChanged(a);
    CompletableFuture<LockResult> fb =
        CompletableFuture.supplyAsync(() -> s2pl.lock("x", b, LockMode.SHARED, LONG));
    CompletableFuture<LockResult> fc =
        CompletableFuture.supplyAsync(() -> s2pl.lock("x", c, LockMode.EXCLUSIVE, LONG));
    assertBlocked(fb);
    assertBlocked(fc);

    a.compareAndSwapStatus(EnumSet.allOf(Status.class), Status.ABORTED);
    assertTrue(s2pl.abortAndRelease(a).isEmpty());

    assertEquals(LockResult.ACQUIRED, fb.get(2, TimeUnit.SECONDS));
    assertEquals(Status.STARTED, b.getStatus(), "b never read a's buffer: nothing to cascade");
    assertEquals(Status.STARTED, c.getStatus());
    assertBlocked(fc);   // c is still behind b, which has not committed
  }

  @Test
  void cascadeReturnsFinishedVictimsNeverTouchesPreparedAndStopsAtABlindWriter() {
    // X(a) <- S(e, p) <- X(w, blind) <- S(r)
    Transaction<String, String> a = txn("a");
    Transaction<String, String> e = txn("e");
    Transaction<String, String> p = txn("p");
    Transaction<String, String> w = txn("w");
    Transaction<String, String> r = txn("r");
    enqueue(a, LockMode.EXCLUSIVE);
    enqueue(e, LockMode.SHARED);
    enqueue(p, LockMode.SHARED);
    enqueue(w, LockMode.EXCLUSIVE);
    enqueue(r, LockMode.SHARED);
    set(e, Status.EXECUTED);
    set(p, Status.PREPARED);

    set(a, Status.ABORTED);
    assertEquals(List.of(e), locks.abortAndRelease(a));

    assertEquals(Status.MUST_ABORT, e.getStatus());
    assertEquals(Status.PREPARED, p.getStatus());
    assertEquals(Status.STARTED, w.getStatus());
    assertEquals(Status.STARTED, r.getStatus());
    assertEquals(List.of(Set.of(e, p), Set.of(w), Set.of(r)), chain());
  }

  @Test
  void prepareWaitsUntilThePredecessorCommitted() throws Exception {
    Transaction<String, String> a = txn("a");
    Transaction<String, String> b = txn("b");
    enqueue(a, LockMode.EXCLUSIVE);
    enqueue(b, LockMode.EXCLUSIVE);
    set(a, Status.PREPARED);
    set(b, Status.EXECUTED);

    CompletableFuture<Boolean> f = CompletableFuture.supplyAsync(() -> locks.awaitPredecessorsResolved(b));
    assertBlocked(f);

    set(a, Status.COMMITTING);
    locks.release(a);
    assertTrue(f.get(2, TimeUnit.SECONDS));
  }

  @Test
  void prepareOfADependentFailsWhenThePredecessorAborts() throws Exception {
    Transaction<String, String> a = txn("a");
    Transaction<String, String> b = txn("b");
    enqueue(a, LockMode.EXCLUSIVE);
    enqueue(b, LockMode.SHARED);
    set(a, Status.EXECUTED);
    set(b, Status.EXECUTED);

    CompletableFuture<Boolean> f = CompletableFuture.supplyAsync(() -> locks.awaitPredecessorsResolved(b));
    assertBlocked(f);

    set(a, Status.ABORTED);
    assertEquals(List.of(b), locks.abortAndRelease(a));
    assertFalse(f.get(2, TimeUnit.SECONDS));
  }

  // ---- get()'s speculative read ----

  @Test
  void readReturnsTheNearestWriterAheadOrNullForTheStore() {
    // X(w1) <- X(w2) <- S(r): r reads w2's buffer, not w1's
    Transaction<String, String> w1 = txn("w1");
    Transaction<String, String> w2 = txn("w2");
    Transaction<String, String> r = txn("r");
    Transaction<String, String> first = txn("first");
    enqueue(w1, LockMode.EXCLUSIVE);
    w1.addToWriteSet("x", "v1");
    enqueue(w2, LockMode.EXCLUSIVE);
    w2.addToWriteSet("x", "v2");
    enqueue(r, LockMode.SHARED);

    assertEquals(LocksMap.ReadResult.of("v2"), locks.read("x", r));

    assertNotNull(locks.enqueue("y", first, LockMode.SHARED));
    assertEquals(LocksMap.ReadResult.of(null), locks.read("y", first), "no writer ahead: store");
  }

  @Test
  void readReturnsTheCachedValueThenDoomedAfterAForeignAbort() {
    Transaction<String, String> r = txn("r");
    enqueue(r, LockMode.SHARED);
    r.addToReadSet("x", "cached");
    assertEquals(LocksMap.ReadResult.of("cached"), locks.read("x", r));

    set(r, Status.ABORTED);
    locks.abortAndRelease(r);

    assertTrue(locks.read("x", r).isDoomed());
  }

  @Test
  void casDoesNotOverwriteAStatusOutsideTheExpectedSet() {
    Transaction<String, String> t = new Transaction<>("t", Status.PREPARED, null);

    Status before = t.compareAndSwapStatus(EnumSet.of(Status.STARTED, Status.EXECUTED), Status.MUST_ABORT);

    assertEquals(Status.PREPARED, before);
    assertEquals(Status.PREPARED, t.getStatus());
    assertTrue(t.compareAndSwapStatus(Status.PREPARED, Status.COMMITTING));
    assertEquals(Status.COMMITTING, t.getStatus());
  }
}
