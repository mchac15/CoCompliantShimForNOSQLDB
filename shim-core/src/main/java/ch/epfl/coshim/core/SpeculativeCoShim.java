package ch.epfl.coshim.core;

import ch.epfl.coshim.store.KvStore;
import java.time.Duration;
import java.util.Objects;

/**
 * The speculative CO-compliant shim of pseudo.txt (S2PL + speculation at {@code executed}).
 *
 * <p>TODO: not implemented yet. This template only wires it into Seata; translate pseudo.txt here
 * (LockNode, chain, locks_map, transactions_map, lock/upgrade/abort_transaction/mark_must_abort),
 * including the tombstones for an abort that arrives before start (see {@link CoShim} and
 * {@link NoCcShim} for a reference of that part).
 */
public class SpeculativeCoShim<K, V> implements CoShim<K, V> {

    private final KvStore<K, V> store;
    private final Duration lockTimeout;

    public SpeculativeCoShim(KvStore<K, V> store, Duration lockTimeout) {
        this.store = Objects.requireNonNull(store);
        this.lockTimeout = Objects.requireNonNull(lockTimeout);
    }

    @Override
    public boolean start(String txnId) {
        throw new UnsupportedOperationException("TODO: pseudo.txt execute (registration)");
    }

    @Override
    public V get(String txnId, K key) {
        throw new UnsupportedOperationException("TODO: pseudo.txt get/lock");
    }

    @Override
    public void put(String txnId, K key, V value) {
        throw new UnsupportedOperationException("TODO: pseudo.txt put/lock/upgrade");
    }

    @Override
    public Outcome end(String txnId) {
        throw new UnsupportedOperationException("TODO: pseudo.txt execute (CAS started -> executed)");
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
