package ch.epfl.coshim.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ch.epfl.coshim.store.InMemoryKvStore;
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
    void startIsOncePerIdAndPrepareBeforeEndVotesNo() {
        shim.start("t1");
        assertFalse(shim.start("t1"));
        assertEquals(Vote.NO, shim.prepare("t1"));
    }
}
