package ch.epfl.coshim.core;

/**
 * A 2PC participant in front of one {@link ch.epfl.coshim.store.KvStore}: the requests of
 * pseudo.txt. The shim never receives or runs client code. Clients reach it through a connection
 * (here {@code coshim-jdbc}'s {@code CoShimConnection}, enlisted in XA branches by Seata) and send it
 * one request at a time for the transaction bound to that connection:
 *
 * <pre>
 *   start(t)                      pseudo.txt start: register t, status started
 *   get(t, k) / put(t, k, v) ...  pseudo.txt get / put, one request per operation
 *   end(t)                        pseudo.txt end: CAS started -> executed
 *   prepare(t)                    pseudo.txt prepare (vote)
 *   commit(t) / abort(t)          pseudo.txt commit / abort (coordinator decision)
 * </pre>
 *
 * <p>Concurrency: many transactions run at the same time, each on its own client connection and
 * thread. A request runs on the thread that sends it, and blocking waits (lock(), prepare()) block
 * only that caller. The shim spawns no threads; its shared state (locks_map, transactions_map) is
 * what makes concurrent transactions conflict and order correctly.
 *
 * <p>Coordinator contract (pseudo.txt, assumption 3): transaction ids are assigned by the
 * coordinator (here derived from the XA Xid), and {@link #start} is called at most once per id.
 * Requests on an unknown or already resolved id take the unknown-id branches: get/put/end fail,
 * prepare votes NO, commit and abort are no-ops.
 *
 * @param <K> key type
 * @param <V> value type
 */
public interface CoShim<K, V> {

    /**
     * Registers the transaction (status {@code started}).
     *
     * @return false if the id is already registered
     */
    boolean start(String txnId);

    /**
     * pseudo.txt {@code get(k)}: SHARED lock, then own buffered write, else the nearest predecessor
     * write, else the store. May block the calling thread.
     *
     * @return the value, or {@code null} if the key has none
     * @throws TxnAbortedException if the transaction must abort (pseudo.txt FAILED); its locks are
     *     already released. Also thrown, without aborting, for a request arriving after {@link #end}:
     *     the write buffer is final once the txn is executed
     */
    V get(String txnId, K key);

    /**
     * pseudo.txt {@code put(k, v)}: EXCLUSIVE lock (or upgrade), then buffer the write. May block the
     * calling thread.
     *
     * @throws TxnAbortedException if the transaction must abort (pseudo.txt FAILED)
     */
    void put(String txnId, K key, V value);

    /**
     * pseudo.txt {@code end(txn_id)}: the client sent its last operation, CAS {@code started →
     * executed}. If the txn was aborted or doomed meanwhile (a cascade or foreign abort landed after
     * the last request), it runs abort_transaction and returns FAILED; an unknown id also returns
     * FAILED. A duplicate end on a txn that is already executed / prepared / committing / committed
     * returns SUCCEEDED and changes nothing: aborting it could abort a prepared txn ("prepared is
     * final").
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
}
