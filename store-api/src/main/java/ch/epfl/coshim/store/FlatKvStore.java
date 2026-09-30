package ch.epfl.coshim.store;

import java.util.Objects;

/**
 * Puts a key-value store that has no table abstraction behind a shim: plain keys go to the store
 * unchanged, and a key that names a table is rejected instead of being silently merged into the
 * flat key space.
 */
public class FlatKvStore<K, V> implements KvStore<TableKey<K>, V> {

    private final KvStore<K, V> store;

    public FlatKvStore(KvStore<K, V> store) {
        this.store = Objects.requireNonNull(store);
    }

    @Override
    public V get(TableKey<K> key) {
        return store.get(plain(key));
    }

    @Override
    public void store(TableKey<K> key, V value) {
        store.store(plain(key), value);
    }

    private K plain(TableKey<K> key) {
        if (key.hasTable()) {
            throw new IllegalArgumentException(
                    "this key-value store has no tables (got table '" + key.table() + "'); use plain keys");
        }
        return key.key();
    }
}
