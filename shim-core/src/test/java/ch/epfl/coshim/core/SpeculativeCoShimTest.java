package ch.epfl.coshim.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import ch.epfl.coshim.store.InMemoryKvStore;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
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
