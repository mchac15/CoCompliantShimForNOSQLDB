package ch.epfl.coshim.core;

/**
 * A 2PC participant in front of one {@link ch.epfl.coshim.store.KvStore}: the entry points of
 * pseudo.txt, split the way XA drives a participant. pseudo.txt's {@code execute(txn_id, code)} is
 * {@link #start} + the code's {@link #get}/{@link #put} calls + {@link #end} (see {@link #execute}).
 *
 * <p>Coordinator contract (pseudo.txt, assumption 3): transaction ids are assigned by the
 * coordinator and {@link #start} is called at most once per id. Calls on an unknown or already
 * resolved id take the unknown-id branches: get/put/end fail, prepare votes NO, commit and abort are
 * no-ops.
 *
 * @param <K> key type
 * @param <V> value type
 */
public interface CoShim<K, V> {

    /**
     * Registers the transaction (status {@code started}); first half of pseudo.txt {@code execute}.
     *
     * @return false if the id is already registered
     */
    boolean start(String txnId);

    /**
     * pseudo.txt {@code get(k)}: SHARED lock, then own buffered write, else the nearest predecessor
     * write, else the store. May block.
     *
     * @return the value, or {@code null} if the key has none
     * @throws TxnAbortedException if the transaction must abort (pseudo.txt FAILED); its locks are
     *     already released
     */
    V get(String txnId, K key);

    /**
     * pseudo.txt {@code put(k, v)}: EXCLUSIVE lock (or upgrade), then buffer the write. May block.
     *
     * @throws TxnAbortedException if the transaction must abort (pseudo.txt FAILED)
     */
    void put(String txnId, K key, V value);

    /**
     * End of pseudo.txt {@code execute}: CAS {@code started → executed}. On failure (the txn was
     * aborted or doomed meanwhile) it aborts the txn.
     */
    Outcome end(String txnId);

    /**
     * pseudo.txt {@code prepare(txn_id)}: blocks until every predecessor has resolved, then votes.
     * Has no timeout of its own; a concurrent {@link #abort} releases it with {@link Vote#NO}.
     */
    Vote prepare(String txnId);

    /** pseudo.txt {@code commit(txn_id)}: applies the write buffer and releases the locks. */
    void commit(String txnId);

    /** pseudo.txt {@code abort(txn_id)}: aborts (cascading to dependents) and releases the locks. */
    void abort(String txnId);

    /** pseudo.txt {@code execute(txn_id, code)}, expressed with the operations above. */
    default Outcome execute(String txnId, TxnCode<K, V> code) {
        if (!start(txnId)) {
            return Outcome.FAILED;
        }
        try {
            code.run(new TxnContext<>() {
                @Override
                public V get(K key) {
                    return CoShim.this.get(txnId, key);
                }

                @Override
                public void put(K key, V value) {
                    CoShim.this.put(txnId, key, value);
                }
            });
        } catch (TxnAbortedException e) {
            abort(txnId);   // idempotent: the failing get/put has already aborted it
            return Outcome.FAILED;
        }
        return end(txnId);
    }
}
