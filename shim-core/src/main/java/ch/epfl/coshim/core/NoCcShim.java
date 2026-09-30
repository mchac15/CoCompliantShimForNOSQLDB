package ch.epfl.coshim.core;

import ch.epfl.coshim.store.KvStore;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

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
 * It follows the {@link CoShim} contract, including the unknown-id branches and the tombstones.
 */
public class NoCcShim<K, V> implements CoShim<K, V> {

    /** Default for {@link #NoCcShim(KvStore, Duration)}: above Seata's default 60 s global timeout. */
    public static final Duration DEFAULT_TOMBSTONE_TTL = Duration.ofMinutes(10);

    private enum Status { STARTED, EXECUTED, PREPARED, TOMBSTONE }

    private static final class Txn<K, V> {
        volatile Status status = Status.STARTED;
        final Map<K, V> writeBuffer = new ConcurrentHashMap<>();
    }

    private record Expiry(String txnId, long atNanos) {}

    private final KvStore<K, V> store;
    private final long tombstoneTtlNanos;

    /**
     * txn id -> txn, or a tombstone: an abort that arrived for an id that was not registered. Keeping
     * tombstones in the same map makes "abort before start" atomic with start: both go through one
     * per-key operation (compute / putIfAbsent), so either start finds the tombstone and fails, or
     * abort finds the txn and removes it.
     */
    private final Map<String, Txn<K, V>> transactions = new ConcurrentHashMap<>();

    /** Tombstones in creation order, to expire them without scanning the map. */
    private final ConcurrentLinkedQueue<Expiry> tombstoneExpiries = new ConcurrentLinkedQueue<>();

    public NoCcShim(KvStore<K, V> store) {
        this(store, DEFAULT_TOMBSTONE_TTL);
    }

    /**
     * @param tombstoneTtl how long an abort-before-start is remembered; at least the coordinator's
     *     global transaction timeout
     */
    public NoCcShim(KvStore<K, V> store, Duration tombstoneTtl) {
        this.store = Objects.requireNonNull(store);
        this.tombstoneTtlNanos = tombstoneTtl.toNanos();
    }

    @Override
    public boolean start(String txnId) {
        // fails on a registered txn and on a tombstone alike
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
        if (txn == null || txn.status == Status.TOMBSTONE) {
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
        if (txn == null || txn.status == Status.TOMBSTONE) {
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
        transactions.remove(txnId, txn);
    }

    @Override
    public void abort(String txnId) {
        long now = System.nanoTime();
        boolean[] tombstoned = {false};
        transactions.compute(txnId, (id, txn) -> {
            if (txn != null && txn.status != Status.TOMBSTONE) {
                return null;   // known txn: drop it (its buffer with it)
            }
            if (txn == null) {
                tombstoned[0] = true;
                txn = new Txn<>();
                txn.status = Status.TOMBSTONE;
            }
            return txn;
        });
        if (tombstoned[0]) {
            tombstoneExpiries.add(new Expiry(txnId, now + tombstoneTtlNanos));
        }
        expireTombstones(now);
    }

    /**
     * Drops tombstones older than the TTL. Most are never consumed by a start (a retried abort of a
     * finished txn, an abort after a failed phase 1), so the TTL is what bounds them.
     *
     * <p>Not definitive: if a start arrives after its tombstone expired (phase 1 stalled longer than
     * the TTL), the txn is accepted although the global transaction was rolled back.
     */
    private void expireTombstones(long now) {
        Expiry head;
        while ((head = tombstoneExpiries.peek()) != null && head.atNanos() - now <= 0) {
            if (tombstoneExpiries.remove(head)) {
                transactions.computeIfPresent(head.txnId(), (id, txn) -> txn.status == Status.TOMBSTONE ? null : txn);
            }
        }
    }

    /** Number of live transactions and tombstones (for tests). */
    public int size() {
        return transactions.size();
    }

    private Txn<K, V> running(String txnId) {
        Txn<K, V> txn = transactions.get(txnId);
        if (txn == null || txn.status != Status.STARTED) {
            throw new TxnAbortedException("txn " + txnId + " is not running");
        }
        return txn;
    }
}
