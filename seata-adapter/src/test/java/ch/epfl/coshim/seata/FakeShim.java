package ch.epfl.coshim.seata;

import ch.epfl.coshim.core.CoShim;
import ch.epfl.coshim.core.Outcome;
import ch.epfl.coshim.core.TxnAbortedException;
import ch.epfl.coshim.core.TxnCode;
import ch.epfl.coshim.core.TxnContext;
import ch.epfl.coshim.core.Vote;
import ch.epfl.coshim.store.KvStore;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Test-only CoShim with no concurrency control: buffers writes, applies them at commit, and records
 * every call. It follows the CoShim contract (unknown ids: prepare votes NO, commit/abort no-op), so
 * it exercises the Seata wiring and nothing else.
 */
class FakeShim implements CoShim<String, String> {

    enum State { EXECUTED, PREPARED }

    final KvStore<String, String> store;
    final List<String> calls = new CopyOnWriteArrayList<>();
    final Map<String, State> txns = new ConcurrentHashMap<>();
    private final Map<String, Map<String, String>> buffers = new ConcurrentHashMap<>();

    volatile boolean voteNo;
    /** Runs at the start of execute, before the txn is registered (to inject a racing TC message). */
    volatile Runnable beforeRegister = () -> {};

    FakeShim(KvStore<String, String> store) {
        this.store = store;
    }

    @Override
    public Outcome execute(String txnId, TxnCode<String, String> code) {
        calls.add("execute:" + txnId);
        beforeRegister.run();
        Map<String, String> buffer = new ConcurrentHashMap<>();
        buffers.put(txnId, buffer);
        txns.put(txnId, State.EXECUTED);
        try {
            code.run(new TxnContext<>() {
                @Override
                public String get(String key) {
                    String own = buffer.get(key);
                    return own != null ? own : store.get(key);
                }

                @Override
                public void put(String key, String value) {
                    buffer.put(key, value);
                }
            });
            return Outcome.SUCCEEDED;
        } catch (TxnAbortedException e) {
            abort(txnId);
            return Outcome.FAILED;
        }
    }

    @Override
    public Vote prepare(String txnId) {
        calls.add("prepare:" + txnId);
        if (!txns.containsKey(txnId)) {
            return Vote.NO;
        }
        if (voteNo) {
            abort(txnId);
            return Vote.NO;
        }
        txns.put(txnId, State.PREPARED);
        return Vote.YES;
    }

    @Override
    public void commit(String txnId) {
        calls.add("commit:" + txnId);
        if (txns.remove(txnId, State.PREPARED)) {
            buffers.remove(txnId).forEach(store::store);
        }
    }

    @Override
    public void abort(String txnId) {
        calls.add("abort:" + txnId);
        txns.remove(txnId);
        buffers.remove(txnId);
    }
}
