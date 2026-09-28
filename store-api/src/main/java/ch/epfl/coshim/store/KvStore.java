package ch.epfl.coshim.store;

/**
 * The whole underlying database, seen as a black-box key-value store. This is the only thing the
 * shim assumes about it (pseudo.txt, "Store"): every backend (in-memory, a real NoSQL store, ...)
 * implements exactly these two operations and nothing else.
 *
 * <ul>
 *   <li>No atomicity across keys, no transactions, no conditional writes.</li>
 *   <li>Both operations are blocking. A backend retries internally until the operation succeeds,
 *       so the protocol has no error path for store failures.</li>
 *   <li>Values are non-null: {@code null} from {@link #get} means "no value for this key".</li>
 * </ul>
 *
 * @param <K> key type
 * @param <V> value type
 */
public interface KvStore<K, V> {

    /**
     * Reads the current value of {@code key}.
     *
     * @return the stored value, or {@code null} if the key has none
     */
    V get(K key);

    /**
     * Blind put: overwrites the value of {@code key}.
     *
     * @param value non-null value
     */
    void store(K key, V value);
}
