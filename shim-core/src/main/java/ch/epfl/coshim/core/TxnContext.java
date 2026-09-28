package ch.epfl.coshim.core;

/**
 * Operations available to {@link TxnCode} inside {@link CoShim#execute}: pseudo.txt {@code get}
 * and {@code put}. Both acquire their lock at the point of access and may block.
 */
public interface TxnContext<K, V> {

    /**
     * pseudo.txt {@code get(k)}: SHARED lock, then own buffered write, else the nearest predecessor
     * write, else the store.
     *
     * @return the value, or {@code null} if the key has none
     * @throws TxnAbortedException if the transaction must abort (pseudo.txt FAILED)
     */
    V get(K key);

    /**
     * pseudo.txt {@code put(k, v)}: EXCLUSIVE lock (or upgrade), then buffer the write.
     *
     * @throws TxnAbortedException if the transaction must abort (pseudo.txt FAILED)
     */
    void put(K key, V value);
}
