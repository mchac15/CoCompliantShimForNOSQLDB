package ch.epfl.coshim.store;

import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * {@link KvStore} backed by a {@link ConcurrentHashMap}. Used for tests and as the no-I/O baseline.
 */
public class InMemoryKvStore<K, V> implements KvStore<K, V> {

    private final ConcurrentMap<K, V> data = new ConcurrentHashMap<>();

    @Override
    public V get(K key) {
        return data.get(key);
    }

    @Override
    public void store(K key, V value) {
        data.put(key, Objects.requireNonNull(value, "values are non-null"));
    }
}
