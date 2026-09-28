package ch.epfl.coshim.core;

/**
 * A 2PC participant in front of one {@link ch.epfl.coshim.store.KvStore}: the entry points of
 * pseudo.txt. Transaction ids are assigned by the coordinator (here: the Seata adapter).
 *
 * <p>Coordinator contract (pseudo.txt, assumption 3): {@link #execute} is called at most once per
 * id; {@link #prepare}, {@link #commit} and {@link #abort} on an unknown or already resolved id take
 * the unknown-id branches (prepare votes NO, commit and abort are no-ops).
 *
 * @param <K> key type
 * @param <V> value type
 */
public interface CoShim<K, V> {

    /**
     * pseudo.txt {@code execute(txn_id, code)}: registers the transaction and runs {@code code},
     * whose reads and writes go through the given {@link TxnContext}. Returns once the transaction
     * is {@code executed} (or failed). May block on conflicting predecessors.
     */
    Outcome execute(String txnId, TxnCode<K, V> code);

    /**
     * pseudo.txt {@code prepare(txn_id)}: blocks until every predecessor has resolved, then votes.
     * Has no timeout of its own; a concurrent {@link #abort} releases it with {@link Vote#NO}.
     */
    Vote prepare(String txnId);

    /** pseudo.txt {@code commit(txn_id)}: applies the write buffer and releases the locks. */
    void commit(String txnId);

    /** pseudo.txt {@code abort(txn_id)}: aborts (cascading to dependents) and releases the locks. */
    void abort(String txnId);
}
