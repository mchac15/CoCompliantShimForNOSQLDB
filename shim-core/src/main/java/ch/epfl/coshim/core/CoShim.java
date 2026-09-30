package ch.epfl.coshim.core;

/**
 * A 2PC participant in front of one database: the requests of pseudo.txt. There is one shim per
 * data source, i.e. per (node, database name); a node can host several databases, each with its own
 * shim, and a shim manages all the tables of its database (its keys are
 * {@link ch.epfl.coshim.store.TableKey (table, key)} pairs). The shim never receives or runs client code. Clients reach it through a connection
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
 * prepare votes NO, commit is a no-op.
 *
 * <p>Tombstones: the coordinator may send {@link #abort} before {@link #start} (Seata's TC can roll a
 * branch back right after registering it, e.g. on a global timeout, before xa start reaches the shim).
 * The shim must therefore remember an abort for an id it does not know, as a tombstone, and refuse a
 * later start of that id, atomically with the registration. Otherwise the txn would execute, vote YES
 * and keep its locks although the global transaction was rolled back. Tombstones expire after a TTL
 * chosen at least as long as the coordinator's global transaction timeout.
 *
 * @param <K> key type
 * @param <V> value type
 */
public interface CoShim<K, V> {

    /**
     * Registers the transaction (status {@code started}).
     *
     * @return false if the id is already registered, or was aborted before it started (tombstone)
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

    /**
     * pseudo.txt {@code abort(txn_id)}: aborts (cascading to dependents) and releases the locks. On an
     * id the shim does not know, it records a tombstone so that a later {@link #start} of the id fails
     * (see the class comment).
     */
    void abort(String txnId);
}
