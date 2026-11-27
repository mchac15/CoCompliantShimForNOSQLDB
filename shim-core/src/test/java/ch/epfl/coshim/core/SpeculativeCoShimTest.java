package ch.epfl.coshim.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ch.epfl.coshim.core.ShimStats.AbortCause;
import ch.epfl.coshim.store.InMemoryKvStore;
import ch.epfl.coshim.types.DeadlockPolicy;
import ch.epfl.coshim.types.SpeculationLimits;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class SpeculativeCoShimTest {

    private static final Duration LOCK_TIMEOUT = Duration.ofSeconds(5);

    private final InMemoryKvStore<String, String> store = new InMemoryKvStore<>();
    private final SpeculativeCoShim<String, String> shim = new SpeculativeCoShim<>(store, LOCK_TIMEOUT);

    @Test
    void requestsThenPrepareCommitApplyBufferedWritesOnlyAtCommit() {
        assertEquals(Outcome.SUCCEEDED, shim.start("t1"));
        shim.put("t1", "x", "1");
        assertEquals("1", shim.get("t1", "x"), "reads its own buffered write");
        assertEquals(Outcome.SUCCEEDED, shim.end("t1"));
        assertNull(store.get("x"));
        assertEquals(Vote.YES, shim.prepare("t1"));
        shim.commit("t1");
        assertEquals("1", store.get("x"));
    }

    @Test
    void readAfterOwnPutReturnsTheWriteNotTheEarlierRead() {
        store.store("x", "0");
        shim.start("t1");
        assertEquals("0", shim.get("t1", "x"));
        shim.put("t1", "x", "1");
        assertEquals("1", shim.get("t1", "x"));
    }

    @Test
    void successorReadsSpeculativelyAsSoonAsThePredecessorEndsAndCommitsAfterIt() throws Exception {
        shim.start("w");
        shim.put("w", "x", "1");
        shim.start("r");
        CompletableFuture<String> read = CompletableFuture.supplyAsync(() -> shim.get("r", "x"));
        Thread.sleep(100);
        assertFalse(read.isDone(), "waits for the writer to finish executing");
        shim.end("w");
        // end() must wake the reader: well before lock_timeout
        assertEquals("1", read.get(1, TimeUnit.SECONDS));
        shim.end("r");

        CompletableFuture<Vote> vote = CompletableFuture.supplyAsync(() -> shim.prepare("r"));
        Thread.sleep(100);
        assertFalse(vote.isDone(), "prepare waits for the predecessor to commit");
        assertEquals(Vote.YES, shim.prepare("w"));
        shim.commit("w");
        assertEquals(Vote.YES, vote.get(1, TimeUnit.SECONDS));
        shim.commit("r");
    }

    @Test
    void abortingTheWriterCascadesToTheSpeculativeReader() {
        shim.start("w");
        shim.put("w", "x", "1");
        shim.end("w");
        shim.start("r");
        assertEquals("1", shim.get("r", "x"));
        shim.end("r");
        shim.abort("w");
        assertEquals(Vote.NO, shim.prepare("r"));
        assertNull(store.get("x"));
    }

    @Test
    void cascadeAndCoordinatorAbortsAreCountedByCause() {
        shim.start("w");
        shim.put("w", "x", "1");
        shim.end("w");
        shim.start("r");
        shim.get("r", "x");
        shim.end("r");
        shim.abort("w");
        shim.prepare("r");
        assertEquals(1, shim.stats().aborts(AbortCause.COORDINATOR));
        assertEquals(1, shim.stats().aborts(AbortCause.CASCADE));
        assertEquals(2, shim.stats().aborts());
        assertEquals(0, shim.stats().commits());
    }

    @Test
    void speculativeGrantIsCountedWithTheWaitItSaved() throws Exception {
        shim.start("w");
        shim.put("w", "x", "1");
        shim.end("w");
        shim.start("r");
        shim.get("r", "x");   // granted on w, executed but not committed: depth 1
        shim.end("r");
        Thread.sleep(50);
        assertEquals(Vote.YES, shim.prepare("w"));
        shim.commit("w");
        assertEquals(Vote.YES, shim.prepare("r"));
        shim.commit("r");
        assertEquals(1, shim.stats().commitsAtDepth(0), "w speculated on nothing");
        assertEquals(1, shim.stats().commitsAtDepth(1));
        assertTrue(shim.stats().savedUsAtDepth(1) >= 50_000, shim.stats().speculationSummary());
        assertEquals(0, shim.stats().savedUsAtDepth(0));
    }

    @Test
    void theNetSavingSubtractsTheWaitMovedToPrepare() throws Exception {
        shim.start("w");
        shim.put("w", "x", "1");
        shim.end("w");
        shim.start("r");
        shim.get("r", "x");
        shim.end("r");
        // r has nothing left to do: its whole wait for w moves into prepare, nothing is saved
        CompletableFuture<Vote> r = CompletableFuture.supplyAsync(() -> shim.prepare("r"));
        Thread.sleep(50);
        assertEquals(Vote.YES, shim.prepare("w"));
        shim.commit("w");
        assertEquals(Vote.YES, r.get(2, TimeUnit.SECONDS));
        shim.commit("r");
        assertTrue(shim.stats().savedUsAtDepth(1) >= 50_000, "gross: " + shim.stats().speculationSummary());
        assertTrue(shim.stats().savedNetUs() < 20_000, "net: " + shim.stats().speculationSummary());
    }

    @Test
    void theNetSavingIsTheGrossOneWhenPrepareDoesNotWait() throws Exception {
        speculativeGrantIsCountedWithTheWaitItSaved();   // r prepares after w committed
        assertTrue(shim.stats().savedNetUs() >= 50_000, shim.stats().speculationSummary());
    }

    @Test
    void aCascadeAbortCountsTheWorkItLost() throws Exception {
        shim.start("w");
        shim.put("w", "x", "1");
        shim.end("w");
        shim.start("r");
        shim.get("r", "x");
        Thread.sleep(30);
        shim.end("r");   // executed on w's value
        shim.abort("w");   // r is doomed and, already executed, aborted at once
        assertEquals(1, shim.stats().aborts(AbortCause.CASCADE));
        assertTrue(shim.stats().cascadeLostUs() >= 30_000, shim.stats().speculationSummary());
        assertTrue(shim.stats().speculationSummary().contains("cascade_lost_us_total="));
    }

    @Test
    void theTimeBudgetSplitsACommittedBranch() throws Exception {
        shim.start("w");
        shim.put("w", "x", "1");
        shim.end("w");
        shim.start("r");
        shim.get("r", "x");
        shim.end("r");
        CompletableFuture<Vote> r = CompletableFuture.supplyAsync(() -> shim.prepare("r"));   // waits for w
        assertEquals(Vote.YES, shim.prepare("w"));
        Thread.sleep(50);   // w voted YES, waiting for the global decision
        shim.commit("w");
        assertEquals(Vote.YES, r.get(2, TimeUnit.SECONDS));
        shim.commit("r");
        assertTrue(shim.stats().committedPostVoteUs() >= 50_000, shim.stats().speculationSummary());
        assertTrue(shim.stats().committedPrepareWaitUs() >= 40_000, "r's wait: " + shim.stats().speculationSummary());
        assertTrue(shim.stats().committedLifetimeUs() >= shim.stats().committedPostVoteUs());
    }

    @Test
    void theTimeBudgetCountsLockWaitsWithoutSpeculation() throws Exception {
        s2pl.start("w");
        s2pl.put("w", "x", "1");
        s2pl.end("w");
        s2pl.start("r");
        CompletableFuture<String> read = CompletableFuture.supplyAsync(() -> s2pl.get("r", "x"));   // blocks on w
        Thread.sleep(50);
        assertEquals(Vote.YES, s2pl.prepare("w"));
        s2pl.commit("w");
        assertEquals("1", read.get(2, TimeUnit.SECONDS));
        s2pl.end("r");
        assertEquals(Vote.YES, s2pl.prepare("r"));
        s2pl.commit("r");
        assertTrue(s2pl.stats().committedLockWaitUs() >= 40_000, s2pl.stats().speculationSummary());
    }

    @Test
    void aWastedBranchCountsUnderItsAbortCause() throws Exception {
        shim.start("t");
        shim.put("t", "x", "1");
        Thread.sleep(30);
        shim.abort("t");
        assertTrue(shim.stats().lostUs(AbortCause.COORDINATOR) >= 30_000, shim.stats().speculationSummary());
        assertEquals(0, shim.stats().lostUs(AbortCause.CASCADE));
        assertTrue(shim.stats().speculationSummary().contains("lost_coordinator_us_total="));
    }

    @Test
    void cascadeFanoutIsCounted() {
        shim.start("w");
        shim.put("w", "x", "1");
        shim.end("w");
        shim.start("r1");
        shim.get("r1", "x");
        shim.start("r2");
        shim.get("r2", "x");
        shim.abort("w");
        assertEquals(1, shim.stats().abortsWithFanout(2), shim.stats().speculationSummary());
    }

    /** No deadlock prevention (the ordered-access runs): lets an older txn wait for a younger one. */
    private final SpeculativeCoShim<String, String> nodp = new SpeculativeCoShim<>(store, LOCK_TIMEOUT,
            SpeculativeCoShim.DEFAULT_TOMBSTONE_TTL, true, DeadlockPolicy.NONE);

    @Test
    void cascadeVictimsAreAttributedToTheCauseAtTheRootOfTheirTree() {
        shim.start("w");
        shim.put("w", "x", "1");
        shim.end("w");
        shim.start("r1");
        shim.get("r1", "x");   // speculates on w
        shim.put("r1", "y", "2");
        shim.end("r1");
        shim.start("r2");
        shim.get("r2", "y");   // speculates on r1: depth 2, w's tree
        shim.end("r2");
        shim.abort("w");
        ShimStats st = shim.stats();
        assertEquals(2, st.aborts(AbortCause.CASCADE), st.speculationSummary());
        assertEquals(2, st.cascadeVictimsWithRoot(AbortCause.COORDINATOR), st.speculationSummary());
        assertEquals(0, st.cascadeVictimsWithRoot(AbortCause.CASCADE));
        assertEquals(1, st.cascadeTreesWithRoot(AbortCause.COORDINATOR));
        assertEquals(2, st.cascadeTreeVictimsWithRoot(AbortCause.COORDINATOR), "r2 counted through r1");
        assertEquals(2, st.cascadeMarked());
        assertEquals(2, st.cascadeMarkedExecuted());
        assertEquals(0, st.cascadeSpared());
        assertTrue(st.speculationSummary().contains("cascade_root_coordinator_victims_total=2"),
                st.speculationSummary());
        assertTrue(st.speculationSummary().contains("cascade_tree=2-3:1"), st.speculationSummary());
    }

    @Test
    void aCascadeSparesATxnStillBlockedInLock() throws Exception {
        store.store("x", "0");
        shim.start("w");
        shim.put("w", "x", "1");
        shim.end("w");
        shim.start("a");
        shim.get("a", "x");   // granted on w
        shim.put("a", "x", "2");   // upgraded in place: a read w's value
        shim.start("b");
        CompletableFuture<String> blocked = CompletableFuture.supplyAsync(() -> shim.get("b", "x"));
        Thread.sleep(100);
        assertFalse(blocked.isDone(), "b waits for a, still running");
        shim.abort("w");
        Thread.sleep(100);
        assertFalse(blocked.isDone(), "b still waits for a, now doomed");
        shim.abort("a");   // e.g. a's own next request
        assertEquals("0", blocked.get(1, TimeUnit.SECONDS), "b read nothing of w's or a's: the committed value");
        ShimStats st = shim.stats();
        assertEquals(1, st.cascadeMarked(), "a only: " + st.speculationSummary());
        assertEquals(1, st.cascadeSpared(), "b read nothing: " + st.speculationSummary());
        assertEquals(0, st.cascadeMarkedExecuted());
        shim.end("b");
        assertEquals(Vote.YES, shim.prepare("b"));
        shim.commit("b");
    }

    @Test
    void theUpgradeLoserIsARootThatNamesItsCauseAndDoomsNobody() throws Exception {
        nodp.start("w");
        nodp.put("w", "z", "1");
        nodp.end("w");
        nodp.start("r1");
        nodp.start("r2");
        nodp.get("r1", "x");
        nodp.get("r2", "x");   // both read x on the same shared node
        nodp.get("r2", "z");   // and r2 speculated on w
        CompletableFuture<Void> upgrade = CompletableFuture.runAsync(() -> nodp.put("r1", "x", "1"));
        Thread.sleep(100);
        assertFalse(upgrade.isDone(), "r1 queues behind r2's read");
        TxnAbortedException e = assertThrows(TxnAbortedException.class, () -> nodp.put("r2", "x", "2"));
        assertTrue(e.getMessage().contains("[cause=upgrade_conflict]"), e.getMessage());
        upgrade.get(1, TimeUnit.SECONDS);
        ShimStats st = nodp.stats();
        assertEquals(1, st.aborts(AbortCause.UPGRADE_CONFLICT));
        assertEquals(0, st.cascadeMarked(), "a txn that never executed has no reader");
        assertEquals(0, st.cascadeTreesWithRoot(AbortCause.UPGRADE_CONFLICT));
    }

    // ---- speculation limits ----

    @Test
    void depthZeroMakesTheSpeculativeShimReadOnlyAfterTheCommitAndCountsTheRefusal() throws Exception {
        SpeculativeCoShim<String, String> capped = new SpeculativeCoShim<>(store, LOCK_TIMEOUT,
                SpeculativeCoShim.DEFAULT_TOMBSTONE_TTL, true, DeadlockPolicy.WAIT_DIE, false,
                new SpeculationLimits(0, SpeculationLimits.NO_LIMIT));
        capped.start("w");
        capped.put("w", "x", "1");
        capped.end("w");
        capped.start("r");
        CompletableFuture<String> read = CompletableFuture.supplyAsync(() -> capped.get("r", "x"));
        Thread.sleep(100);
        assertFalse(read.isDone(), "an executed writer is not enough at depth 0");
        assertEquals(Vote.YES, capped.prepare("w"));
        Thread.sleep(100);
        assertFalse(read.isDone(), "nor is a prepared one");
        capped.commit("w");
        assertEquals("1", read.get(1, TimeUnit.SECONDS));
        capped.end("r");
        assertEquals(Vote.YES, capped.prepare("r"));
        capped.commit("r");
        assertEquals(1, capped.stats().refusedByDepth());
        assertTrue(capped.stats().speculationSummary().contains("spec_refused_depth_total=1"));
    }

    // ---- non-speculative baseline: same shim, lock() waits for the predecessors to commit ----

    private final SpeculativeCoShim<String, String> s2pl =
            new SpeculativeCoShim<>(store, LOCK_TIMEOUT, SpeculativeCoShim.DEFAULT_TOMBSTONE_TTL, false);

    @Test
    void nonSpeculativeReaderWaitsForTheWriterToCommitAndReadsTheStore() throws Exception {
        s2pl.start("w");
        s2pl.put("w", "x", "1");
        s2pl.end("w");
        s2pl.start("r");
        CompletableFuture<String> read = CompletableFuture.supplyAsync(() -> s2pl.get("r", "x"));
        Thread.sleep(100);
        assertFalse(read.isDone(), "an executed writer is not enough without speculation");
        assertEquals(Vote.YES, s2pl.prepare("w"));
        Thread.sleep(100);
        assertFalse(read.isDone(), "nor is a prepared one");
        s2pl.commit("w");
        assertEquals("1", read.get(1, TimeUnit.SECONDS));
        s2pl.end("r");
        assertEquals(Vote.YES, s2pl.prepare("r"));
        s2pl.commit("r");
        assertEquals(2, s2pl.stats().commits());
    }

    @Test
    void nonSpeculativeWriterAbortNeverCascades() throws Exception {
        store.store("x", "0");
        s2pl.start("w");
        s2pl.put("w", "x", "1");
        s2pl.end("w");
        s2pl.start("r");
        CompletableFuture<String> read = CompletableFuture.supplyAsync(() -> s2pl.get("r", "x"));
        Thread.sleep(100);
        assertFalse(read.isDone());
        s2pl.abort("w");
        assertEquals("0", read.get(1, TimeUnit.SECONDS), "reads the committed value, never w's buffer");
        s2pl.end("r");
        assertEquals(Vote.YES, s2pl.prepare("r"));
        s2pl.commit("r");
        assertEquals(0, s2pl.stats().aborts(AbortCause.CASCADE));
        assertEquals(1, s2pl.stats().aborts(AbortCause.COORDINATOR));
    }

    @Test
    void nonSpeculativeWaitTimesOutIfTheWriterNeverResolves() {
        SpeculativeCoShim<String, String> shortTimeout = new SpeculativeCoShim<>(
                store, Duration.ofMillis(100), SpeculativeCoShim.DEFAULT_TOMBSTONE_TTL, false);
        shortTimeout.start("w");
        shortTimeout.put("w", "x", "1");
        shortTimeout.end("w");
        shortTimeout.start("r");
        assertThrows(TxnAbortedException.class, () -> shortTimeout.put("r", "x", "2"));
        assertEquals(1, shortTimeout.stats().aborts(AbortCause.LOCK_TIMEOUT));
        assertEquals(Vote.YES, shortTimeout.prepare("w"), "the timeout only aborts the waiter");
    }

    @Test
    void woundWaitAbortsTheYoungerRunningWriterAndTheOlderTxnCommits() {
        SpeculativeCoShim<String, String> ww = new SpeculativeCoShim<>(
                store, LOCK_TIMEOUT, SpeculativeCoShim.DEFAULT_TOMBSTONE_TTL, true, DeadlockPolicy.WOUND_WAIT);
        store.store("x", "0");
        ww.start("old");
        ww.start("young");
        ww.put("young", "x", "1");
        ww.put("young", "y", "1");

        assertEquals("0", ww.get("old", "x"), "young is wounded and released: old reads the store");
        assertEquals(1, ww.stats().aborts(AbortCause.WOUNDED));
        assertEquals(0, ww.stats().aborts(AbortCause.CASCADE), "the wounder is not doomed by the release");

        assertThrows(TxnAbortedException.class, () -> ww.put("young", "z", "1"));
        assertEquals(Outcome.FAILED, ww.end("young"));
        assertEquals(Vote.NO, ww.prepare("young"));
        ww.abort("young");   // the coordinator's rollback: nothing left to do
        assertEquals(1, ww.stats().aborts(), "counted once, as a wound");

        ww.put("old", "y", "2");
        assertEquals(Outcome.SUCCEEDED, ww.end("old"));
        assertEquals(Vote.YES, ww.prepare("old"));
        ww.commit("old");
        assertEquals("0", store.get("x"));
        assertEquals("2", store.get("y"));
    }

    @Test
    void requestsAfterEndFailWithoutAbortingTheTxn() {
        shim.start("t1");
        shim.put("t1", "x", "1");
        shim.end("t1");
        assertThrows(TxnAbortedException.class, () -> shim.put("t1", "x", "2"));
        assertThrows(TxnAbortedException.class, () -> shim.get("t1", "x"));
        assertEquals(Vote.YES, shim.prepare("t1"));
        shim.commit("t1");
        assertEquals("1", store.get("x"));
    }

    @Test
    void requestsOnAnUnknownIdFail() {
        assertThrows(TxnAbortedException.class, () -> shim.get("nope", "x"));
        assertThrows(TxnAbortedException.class, () -> shim.put("nope", "x", "1"));
        assertEquals(Outcome.FAILED, shim.end("nope"));
        assertEquals(Vote.NO, shim.prepare("nope"));
    }

    @Test
    void abortBeforeStartLeavesATombstoneThatRejectsTheStart() {
        shim.abort("t1");
        assertEquals(Outcome.FAILED, shim.start("t1"));
    }

    @Test
    void tombstoneGcNeverEvictsALiveTxn() throws Exception {
        SpeculativeCoShim<String, String> expiring =
                new SpeculativeCoShim<>(store, LOCK_TIMEOUT, Duration.ofMillis(1));
        expiring.start("t1");
        expiring.put("t1", "x", "1");
        expiring.end("t1");
        expiring.abort("dead");
        Thread.sleep(1500); // at least one gc period
        assertEquals(Outcome.SUCCEEDED, expiring.start("dead"), "the expired tombstone is gone");
        assertEquals(Vote.YES, expiring.prepare("t1"));
        expiring.commit("t1");
        assertEquals("1", store.get("x"));
    }
}
