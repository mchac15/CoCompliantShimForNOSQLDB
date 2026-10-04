package ch.epfl.coshim.store;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

class FlatKvStoreTest {

    private final InMemoryKvStore<String, String> plain = new InMemoryKvStore<>();
    private final FlatKvStore<String, String> store = new FlatKvStore<>(plain);

    @Test
    void plainKeysGoToTheStoreUnchanged() {
        store.store(TableKey.of("x"), "1");
        assertEquals("1", plain.get("x"));
        assertEquals("1", store.get(TableKey.of("x")));
    }

    @Test
    void tableKeysAreRejectedByAStoreWithoutTables() {
        assertThrows(IllegalArgumentException.class, () -> store.store(new TableKey<>("orders", "x"), "1"));
        assertThrows(IllegalArgumentException.class, () -> store.get(new TableKey<>("orders", "x")));
    }
}
