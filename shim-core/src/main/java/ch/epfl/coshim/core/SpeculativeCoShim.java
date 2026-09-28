package ch.epfl.coshim.core;

import ch.epfl.coshim.store.KvStore;
import java.time.Duration;
import java.util.Objects;

/**
 * The speculative CO-compliant shim of pseudo.txt (S2PL + speculation at {@code executed}).
 *
 * <p>TODO: not implemented yet. This template only wires it into Seata; translate pseudo.txt here
 * (LockNode, chain, locks_map, transactions_map, lock/upgrade/abort_transaction/mark_must_abort).
 */
public class SpeculativeCoShim<K, V> implements CoShim<K, V> {

    private final KvStore<K, V> store;
    private final Duration lockTimeout;

    public SpeculativeCoShim(KvStore<K, V> store, Duration lockTimeout) {
        this.store = Objects.requireNonNull(store);
        this.lockTimeout = Objects.requireNonNull(lockTimeout);
    }

    @Override
    public Outcome execute(String txnId, TxnCode<K, V> code) {
        throw new UnsupportedOperationException("TODO: pseudo.txt execute/get/put/lock/upgrade");
    }

    @Override
    public Vote prepare(String txnId) {
        throw new UnsupportedOperationException("TODO: pseudo.txt prepare");
    }

    @Override
    public void commit(String txnId) {
        throw new UnsupportedOperationException("TODO: pseudo.txt commit");
    }

    @Override
    public void abort(String txnId) {
        throw new UnsupportedOperationException("TODO: pseudo.txt abort/abort_transaction");
    }
}
