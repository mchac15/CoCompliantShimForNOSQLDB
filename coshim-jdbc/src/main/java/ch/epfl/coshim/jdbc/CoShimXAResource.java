package ch.epfl.coshim.jdbc;

import ch.epfl.coshim.core.CoShim;
import ch.epfl.coshim.core.Outcome;
import ch.epfl.coshim.core.Vote;
import java.nio.charset.StandardCharsets;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

/**
 * The shim as an XA participant. Seata's {@code ConnectionProxyXA} / {@code ResourceManagerXA} drive
 * it exactly like a MySQL/PG XAResource:
 *
 * <pre>
 *   start(xid, TMNOFLAGS)   shim.start          (txn registered, status started)
 *   ...KvSession get/put... shim.get / shim.put
 *   end(xid, TMSUCCESS)     shim.end            (started -> executed; FAILED -> XA_RBROLLBACK)
 *   end(xid, TMFAIL)        (rollback follows)
 *   prepare(xid)            shim.prepare        (YES -> XA_OK, NO -> XA_RBROLLBACK)
 *   commit(xid, false)      shim.commit
 *   rollback(xid)           shim.abort
 * </pre>
 *
 * The shim txn id is derived from the Xid (Seata: global xid + branch id), so a phase-2 call on a
 * fresh connection, or from another application instance, finds the same txn on the shim.
 *
 * <p>This class keeps no transaction state of its own. In particular a rollback from the TC that
 * arrives before xa start, or while phase 1 is still running, is handled by the shim: its abort
 * either aborts the registered txn or leaves a tombstone that makes the later start fail (see
 * {@link CoShim}). Every phase-1 step after that fails on the shim's answer.
 */
public class CoShimXAResource implements XAResource {

    private final CoShimConnection<?, ?> connection;
    private final CoShim<?, ?> shim;

    CoShimXAResource(CoShimConnection<?, ?> connection) {
        this.connection = connection;
        this.shim = connection.getDataSource().getShim();
    }

    /** The shim txn id of an XA branch. */
    public static String txnId(Xid xid) {
        return xid.getFormatId() + ":" + new String(xid.getGlobalTransactionId(), StandardCharsets.UTF_8) + ":"
                + new String(xid.getBranchQualifier(), StandardCharsets.UTF_8);
    }

    @Override
    public void start(Xid xid, int flags) throws XAException {
        if (flags != TMNOFLAGS) {
            throw xaError(XAException.XAER_INVAL, "only TMNOFLAGS is supported (no join/resume)");
        }
        if (connection.inLocalTransaction()) {
            throw xaError(XAException.XAER_OUTSIDE, "connection has a local transaction in progress");
        }
        String txnId = txnId(xid);
        if (!shim.start(txnId)) {
            // the coordinator already rolled the branch back (tombstone on the shim), or a duplicate start
            throw xaError(XAException.XA_RBROLLBACK, "branch " + txnId + " was rolled back before it started");
        }
        // This connection now sends its get/put requests for txnId, until end(). One connection
        // serves one branch at a time (as in JDBC/XA for any database); concurrent transactions use
        // different connections (one per thread / global transaction), all reaching the same shim.
        connection.bind(txnId);
    }

    @Override
    public void end(Xid xid, int flags) throws XAException {
        String txnId = txnId(xid);
        if (flags == TMFAIL) {
            // rollback(xid) follows. Not serialized with the connection's requests: the abort must
            // reach the shim even while a get/put of the branch is blocked there
            connection.unbind(txnId);
            return;
        }
        if (flags != TMSUCCESS) {
            connection.unbind(txnId);
            throw xaError(XAException.XAER_INVAL, "only TMSUCCESS/TMFAIL are supported (no suspend)");
        }
        // waits for a get/put of the branch still in flight on another thread: the shim's end must
        // come after the last get/put has returned (see CoShimConnection)
        if (connection.endBranch(txnId) == Outcome.FAILED) {
            throw xaError(XAException.XA_RBROLLBACK, "branch " + txnId + " was aborted");
        }
    }

    @Override
    public int prepare(Xid xid) throws XAException {
        String txnId = txnId(xid);
        if (shim.prepare(txnId) == Vote.NO) {
            throw xaError(XAException.XA_RBROLLBACK, "branch " + txnId + " voted NO");
        }
        // never XA_RDONLY: even a read-only branch holds its place in the lock chains until commit
        return XA_OK;
    }

    @Override
    public void commit(Xid xid, boolean onePhase) throws XAException {
        // XA's one-phase optimisation: a transaction manager with a single participant may skip
        // prepare and ask for commit(xid, onePhase = true) directly. The resource must then do both
        // itself, so we still run the shim's prepare (wait for predecessors, vote) before committing;
        // a NO vote throws XA_RBROLLBACK and nothing is committed. Seata always calls
        // commit(xid, false) after its own phase-1 prepare, so with Seata this branch is never taken.
        if (onePhase) {
            prepare(xid);
        }
        shim.commit(txnId(xid));
    }

    @Override
    public void rollback(Xid xid) {
        shim.abort(txnId(xid));
    }

    @Override
    public void forget(Xid xid) {}

    @Override
    public Xid[] recover(int flag) {
        return new Xid[0];   // no durability, nothing to recover (pseudo.txt: no crash recovery)
    }

    @Override
    public boolean isSameRM(XAResource other) {
        return other instanceof CoShimXAResource && ((CoShimXAResource) other).shim == shim;
    }

    @Override
    public int getTransactionTimeout() {
        return 0;
    }

    @Override
    public boolean setTransactionTimeout(int seconds) {
        return false;
    }

    private static XAException xaError(int errorCode, String message) {
        XAException e = new XAException(message);
        e.errorCode = errorCode;
        return e;
    }
}
