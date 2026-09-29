package ch.epfl.coshim.core;

import ch.epfl.coshim.store.KvStore;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A shim with <b>no concurrency control</b>: writes are buffered and applied at commit, nothing is
 * locked or ordered. It is NOT serializable (and not CO); it exists as
 *
 * <ul>
 *   <li>the pass-through baseline for benchmarks: same buffering and XA plumbing as the real shim,
 *       so the difference measures the cost of the concurrency control alone;</li>
 *   <li>a stand-in to test the XA / Seata wiring before {@link SpeculativeCoShim} exists.</li>
 * </ul>
 *
 * It follows the {@link CoShim} contract, including the unknown-id branches.
 */
public class NoCcShim<K, V> implements CoShim<K, V> {

    private enum Status { STARTED, EXECUTED, PREPARED }

    private static final class Txn<K, V> {
        volatile Status status = Status.STARTED;
        final Map<K, V> writeBuffer = new ConcurrentHashMap<>();
    }

    private final KvStore<K, V> store;
    private final Map<String, Txn<K, V>> transactions = new ConcurrentHashMap<>();

    public NoCcShim(KvStore<K, V> store) {
        this.store = Objects.requireNonNull(store);
    }

    @Override
    public boolean start(String txnId) {
        return transactions.putIfAbsent(txnId, new Txn<>()) == null;
    }

    @Override
    public V get(String txnId, K key) {
        V own = running(txnId).writeBuffer.get(key);
        return own != null ? own : store.get(key);
    }

    @Override
    public void put(String txnId, K key, V value) {
        running(txnId).writeBuffer.put(key, Objects.requireNonNull(value, "values are non-null"));
    }

    @Override
    public Outcome end(String txnId) {
        Txn<K, V> txn = transactions.get(txnId);
        if (txn == null) {
            return Outcome.FAILED;
        }
        if (txn.status != Status.STARTED) {
            return Outcome.SUCCEEDED;   // duplicate end: already executed or prepared, never abort it
        }
        txn.status = Status.EXECUTED;
        return Outcome.SUCCEEDED;
    }

    @Override
    public Vote prepare(String txnId) {
        Txn<K, V> txn = transactions.get(txnId);
        if (txn == null) {
            return Vote.NO;
        }
        if (txn.status == Status.STARTED) {
            abort(txnId);
            return Vote.NO;
        }
        txn.status = Status.PREPARED;
        return Vote.YES;
    }

    @Override
    public void commit(String txnId) {
        Txn<K, V> txn = transactions.get(txnId);
        if (txn == null || txn.status != Status.PREPARED) {
            return;
        }
        txn.writeBuffer.forEach(store::store);
        transactions.remove(txnId);
    }

    @Override
    public void abort(String txnId) {
        transactions.remove(txnId);
    }

    private Txn<K, V> running(String txnId) {
        Txn<K, V> txn = transactions.get(txnId);
        if (txn == null || txn.status != Status.STARTED) {
            throw new TxnAbortedException("txn " + txnId + " is not running");
        }
        return txn;
    }
}
