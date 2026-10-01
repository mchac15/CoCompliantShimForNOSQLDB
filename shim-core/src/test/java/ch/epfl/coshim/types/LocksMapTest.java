package ch.epfl.coshim.types;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ch.epfl.coshim.types.LockType.LockMode;
import ch.epfl.coshim.types.Transaction.Status;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;

class LocksMapTest {

  private final LocksMap<String, String> locks = new LocksMap<>();

  private static Transaction<String, String> txn(String id) {
    return new Transaction<>(id, Status.STARTED, null);
  }

  /** The holders of each node, head to tail, walking next; checks prev/head/tail agree. */
  private List<Set<Transaction<String, String>>> chain(String key) {
    Chain<String, String> chain = locks.getChain(key);
    List<Set<Transaction<String, String>>> out = new ArrayList<>();
    synchronized (chain) {
      LockNode<String, String> prev = null;
      for (LockNode<String, String> n = chain.getHead(); n != null; n = n.getNext()) {
        assertSame(prev, n.getPrev(), "prev pointer disagrees with next");
        assertSame(chain, n.getChain());
        out.add(Set.copyOf(n.getHolders()));
        prev = n;
      }
      assertSame(prev, chain.lastNode(), "tail disagrees with the last node");
    }
    return out;
  }

  @Test
  void releasingTheLastNodeRemovesTheChainAndANewRequestGetsAFreshOne() {
    Transaction<String, String> a = txn("a");
    LockNode<String, String> node = locks.acquire("x", a, LockMode.EXCLUSIVE);
    Chain<String, String> old = node.getChain();

    locks.release(node, a);

    assertFalse(locks.containsKey("x"));
    Transaction<String, String> b = txn("b");
    assertTrue(locks.acquire("x", b, LockMode.SHARED).getChain() != old);
    assertEquals(List.of(Set.of(b)), chain("x"));
  }

  @Test
  void releasingANonHeadNodeKeepsTheHead() {
    Transaction<String, String> a = txn("a");
    Transaction<String, String> b = txn("b");
    Transaction<String, String> c = txn("c");
    locks.acquire("x", a, LockMode.EXCLUSIVE);
    LockNode<String, String> nb = locks.acquire("x", b, LockMode.EXCLUSIVE);
    locks.acquire("x", c, LockMode.EXCLUSIVE);

    locks.release(nb, b);

    assertEquals(List.of(Set.of(a), Set.of(c)), chain("x"));
  }

  @Test
  void doomedTxnDoesNotAppend() {
    Transaction<String, String> a = txn("a");
    a.compareAndSwapStatus(EnumSet.of(Status.STARTED, Status.EXECUTED), Status.MUST_ABORT);

    assertNull(locks.acquire("x", a, LockMode.SHARED));
    assertNull(a.getLock("x"));
  }

  @Test
  void upgradeInPlaceWhenSoleHolder() {
    Transaction<String, String> t = txn("t");
    LockNode<String, String> node = locks.acquire("x", t, LockMode.SHARED);

    assertEquals(Chain.UpgradeResult.UPGRADED, locks.upgrade("x", t));

    assertSame(node, t.getLock("x").node());
    assertEquals(LockMode.EXCLUSIVE, t.getLock("x").mode());
    assertTrue(node.isUpgraded());
  }

  @Test
  void upgradeScansFromItsOwnNodeNotTheHead() {
    // S(a) <- X(b, blind) <- S(t, c): t must stay behind b, it read b's value
    Transaction<String, String> a = txn("a");
    Transaction<String, String> b = txn("b");
    Transaction<String, String> t = txn("t");
    Transaction<String, String> c = txn("c");
    locks.acquire("x", a, LockMode.SHARED);
    locks.acquire("x", b, LockMode.EXCLUSIVE);
    locks.acquire("x", t, LockMode.SHARED);
    locks.acquire("x", c, LockMode.SHARED);

    assertEquals(Chain.UpgradeResult.UPGRADED_MUST_WAIT, locks.upgrade("x", t));

    assertEquals(List.of(Set.of(a), Set.of(b), Set.of(c), Set.of(t)), chain("x"));
    assertEquals(LockMode.EXCLUSIVE, t.getLock("x").mode());
    assertTrue(t.getLock("x").node().isUpgraded());
  }

  @Test
  void upgradeInFrontOfABlindWriterIsLinkedForward() {
    // S(t, r) <- X(w, blind): t goes between its readers' node and w
    Transaction<String, String> t = txn("t");
    Transaction<String, String> r = txn("r");
    Transaction<String, String> w = txn("w");
    LockNode<String, String> shared = locks.acquire("x", t, LockMode.SHARED);
    locks.acquire("x", r, LockMode.SHARED);
    LockNode<String, String> nw = locks.acquire("x", w, LockMode.EXCLUSIVE);

    assertEquals(Chain.UpgradeResult.UPGRADED_MUST_WAIT, locks.upgrade("x", t));

    LockNode<String, String> nt = t.getLock("x").node();
    synchronized (nt.getChain()) {
      assertSame(nt, shared.getNext());
      assertSame(nw, nt.getNext());
      assertSame(nt, nw.getPrev());
    }
    assertEquals(List.of(Set.of(r), Set.of(t), Set.of(w)), chain("x"));
  }

  @Test
  void upgradeSplicesOutItsOwnEmptyNodeOnlyAndKeepsTheHead() {
    // X(h) <- S(t) <- X(w) <- S(r), then w aborts: X(h) <- S(t) <- S(r)
    Transaction<String, String> h = txn("h");
    Transaction<String, String> t = txn("t");
    Transaction<String, String> w = txn("w");
    Transaction<String, String> r = txn("r");
    locks.acquire("x", h, LockMode.EXCLUSIVE);
    locks.acquire("x", t, LockMode.SHARED);
    LockNode<String, String> nw = locks.acquire("x", w, LockMode.EXCLUSIVE);
    locks.acquire("x", r, LockMode.SHARED);
    locks.release(nw, w);

    assertEquals(Chain.UpgradeResult.UPGRADED_MUST_WAIT, locks.upgrade("x", t));

    assertEquals(List.of(Set.of(h), Set.of(r), Set.of(t)), chain("x"));
  }

  @Test
  void upgradeBehindAnotherUpgraderAborts() {
    // S(t, u) where u upgraded first: S(t) <- X(u, upgraded); t loses
    Transaction<String, String> t = txn("t");
    Transaction<String, String> u = txn("u");
    locks.acquire("x", t, LockMode.SHARED);
    locks.acquire("x", u, LockMode.SHARED);
    assertEquals(Chain.UpgradeResult.UPGRADED_MUST_WAIT, locks.upgrade("x", u));

    assertEquals(Chain.UpgradeResult.MUST_ABORT, locks.upgrade("x", t));
    assertEquals(LockMode.SHARED, t.getLock("x").mode());
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
