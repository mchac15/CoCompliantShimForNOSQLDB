package ch.epfl.coshim.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import ch.epfl.coshim.store.InMemoryKvStore;
import org.junit.jupiter.api.Test;

class NoCcShimTest {

    private final InMemoryKvStore<String, String> store = new InMemoryKvStore<>();
    private final NoCcShim<String, String> shim = new NoCcShim<>(store);

    @Test
    void executePrepareCommitAppliesBufferedWritesOnlyAtCommit() {
        assertEquals(Outcome.SUCCEEDED, shim.execute("t1", ctx -> {
            ctx.put("x", "1");
            assertEquals("1", ctx.get("x"), "reads its own buffered write");
        }));
        assertNull(store.get("x"));
        assertEquals(Vote.YES, shim.prepare("t1"));
        shim.commit("t1");
        assertEquals("1", store.get("x"));
    }

    @Test
    void abortDiscardsTheBuffer() {
        shim.execute("t1", ctx -> ctx.put("x", "1"));
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
    void startIsOncePerIdAndPrepareBeforeEndVotesNo() {
        shim.start("t1");
        assertFalse(shim.start("t1"));
        assertEquals(Vote.NO, shim.prepare("t1"));
    }
}
