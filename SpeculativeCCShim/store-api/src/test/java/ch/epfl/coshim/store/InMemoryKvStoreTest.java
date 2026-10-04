package ch.epfl.coshim.store;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

class InMemoryKvStoreTest {

    @Test
    void getReturnsNullForAbsentKeyAndLastStoredValueOtherwise() {
        KvStore<String, String> store = new InMemoryKvStore<>();
        assertNull(store.get("x"));
        store.store("x", "1");
        store.store("x", "2");
        assertEquals("2", store.get("x"));
    }

    @Test
    void rejectsNullValues() {
        KvStore<String, String> store = new InMemoryKvStore<>();
        assertThrows(NullPointerException.class, () -> store.store("x", null));
    }
}
