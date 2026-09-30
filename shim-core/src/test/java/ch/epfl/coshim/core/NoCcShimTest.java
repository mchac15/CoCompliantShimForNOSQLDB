package ch.epfl.coshim.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ch.epfl.coshim.store.InMemoryKvStore;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class NoCcShimTest {

    private final InMemoryKvStore<String, String> store = new InMemoryKvStore<>();
    private final NoCcShim<String, String> shim = new NoCcShim<>(store);

    @Test
    void requestsThenPrepareCommitApplyBufferedWritesOnlyAtCommit() {
        assertTrue(shim.start("t1"));
        shim.put("t1", "x", "1");
        assertEquals("1", shim.get("t1", "x"), "reads its own buffered write");
        assertEquals(Outcome.SUCCEEDED, shim.end("t1"));
        assertNull(store.get("x"));
        assertEquals(Vote.YES, shim.prepare("t1"));
        shim.commit("t1");
        assertEquals("1", store.get("x"));
    }

    @Test
    void commitSendsTheWriteBufferToTheStoreAsOneBatch() {
        List<Map<String, String>> batches = new ArrayList<>();
        NoCcShim<String, String> batching = new NoCcShim<>(new InMemoryKvStore<>() {
            @Override
            public void storeAll(Map<String, String> entries) {
                batches.add(Map.copyOf(entries));
                super.storeAll(entries);
            }
        });
        batching.start("t1");
        batching.put("t1", "x", "1");
        batching.put("t1", "y", "2");
        batching.end("t1");
        batching.prepare("t1");
        batching.commit("t1");
        assertEquals(List.of(Map.of("x", "1", "y", "2")), batches);
    }

    @Test
    void abortDiscardsTheBuffer() {
        shim.start("t1");
        shim.put("t1", "x", "1");
        shim.end("t1");
        shim.abort("t1");
        shim.commit("t1");
        assertNull(store.get("x"));
    }

    @Test
    void unknownIdsTakeTheUnknownIdBranches() {
        assertEquals(Vote.NO, shim.prepare("nope"));
        shim.commit("nope");
        shim.abort("nope");
        assertEquals(Outcome.FAILED, shim.end("nope"));
        assertThrows(TxnAbortedException.class, () -> shim.get("nope", "x"));
    }

    @Test
    void duplicateEndNeverAbortsAPreparedTxnAndLateRequestsFail() {
        shim.start("t1");
        shim.put("t1", "x", "1");
        shim.end("t1");
        assertThrows(TxnAbortedException.class, () -> shim.put("t1", "y", "2"), "buffer is final after end");
        assertEquals(Vote.YES, shim.prepare("t1"));
        assertEquals(Outcome.SUCCEEDED, shim.end("t1"), "duplicate end");
        shim.commit("t1");
        assertEquals("1", store.get("x"), "still committable: prepared is final");
        assertNull(store.get("y"));
    }

    @Test
    void abortBeforeStartLeavesATombstoneThatRefusesTheStart() {
        shim.abort("t1");   // the coordinator rolled the branch back before its start arrived
        assertFalse(shim.start("t1"));
        assertThrows(TxnAbortedException.class, () -> shim.put("t1", "x", "1"));
        assertEquals(Outcome.FAILED, shim.end("t1"));
        assertEquals(Vote.NO, shim.prepare("t1"));
        shim.commit("t1");
        assertNull(store.get("x"));
    }

    @Test
    void abortOfAKnownTxnLeavesNoTombstone() {
        shim.start("t1");
        shim.abort("t1");
        assertEquals(0, shim.size());
    }

    @Test
    void tombstonesExpireAfterTheTtl() {
        NoCcShim<String, String> expiring = new NoCcShim<>(store, Duration.ZERO);
        expiring.abort("t1");
        assertEquals(0, expiring.size(), "expired right away with a zero TTL");
        assertTrue(expiring.start("t1"), "the documented caveat: a start after expiry is accepted");
    }

    @Test
    void startIsOncePerIdAndPrepareBeforeEndVotesNo() {
        shim.start("t1");
        assertFalse(shim.start("t1"));
        assertEquals(Vote.NO, shim.prepare("t1"));
    }
}
