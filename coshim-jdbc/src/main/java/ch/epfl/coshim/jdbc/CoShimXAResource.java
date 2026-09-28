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
 * fresh connection finds the same txn. {@link BranchStates} covers a TC rollback racing phase 1.
 */
public class CoShimXAResource implements XAResource {

    private final CoShimConnection<?, ?> connection;
    private final CoShim<?, ?> shim;
    private final BranchStates states;

    /** Txn this resource ended with TMFAIL while associated: its rollback is local (phase 1). */
    private volatile String failedLocally;

    CoShimXAResource(CoShimConnection<?, ?> connection) {
        this.connection = connection;
        this.shim = connection.getDataSource().getShim();
        this.states = connection.getDataSource().getBranchStates();
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
        String txnId = txnId(xid);
        if (!states.activate(txnId)) {
            throw xaError(XAException.XA_RBROLLBACK, "branch " + txnId + " was already rolled back by the coordinator");
        }
        if (!shim.start(txnId)) {
            throw xaError(XAException.XAER_DUPID, "branch " + txnId + " already started");
        }
        if (states.isRolledBack(txnId)) {
            // the TC's rollback ran between activate and shim.start: its abort found nothing
            abortObserved(txnId);
            throw xaError(XAException.XA_RBROLLBACK, "branch " + txnId + " rolled back by the coordinator");
        }
        // This connection now sends its get/put requests for txnId, until end(). One connection
        // serves one branch at a time (as in JDBC/XA for any database); concurrent transactions use
        // different connections (one per thread / global transaction), all sharing the same shim.
        connection.bind(txnId);
    }

    @Override
    public void end(Xid xid, int flags) throws XAException {
        String txnId = txnId(xid);
        boolean associated = connection.unbind(txnId);
        if (flags == TMFAIL) {
            if (associated) {
                failedLocally = txnId;
            }
            return;   // rollback(xid) follows
        }
        if (flags != TMSUCCESS) {
            throw xaError(XAException.XAER_INVAL, "only TMSUCCESS/TMFAIL are supported (no suspend)");
        }
        Outcome outcome = shim.end(txnId);
        if (states.isRolledBack(txnId)) {
            abortObserved(txnId);
            throw xaError(XAException.XA_RBROLLBACK, "branch " + txnId + " rolled back by the coordinator");
        }
        if (outcome == Outcome.FAILED) {
            states.finished(txnId);   // the shim already aborted it
            throw xaError(XAException.XA_RBROLLBACK, "branch " + txnId + " was aborted by the shim");
        }
    }

    @Override
    public int prepare(Xid xid) throws XAException {
        String txnId = txnId(xid);
        Vote vote = shim.prepare(txnId);
        if (states.isRolledBack(txnId)) {
            abortObserved(txnId);
            throw xaError(XAException.XA_RBROLLBACK, "branch " + txnId + " rolled back by the coordinator");
        }
        if (vote == Vote.NO) {
            states.finished(txnId);   // the shim already aborted it
            throw xaError(XAException.XA_RBROLLBACK, "branch " + txnId + " voted NO");
        }
        states.votedYes(txnId);
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
        String txnId = txnId(xid);
        shim.commit(txnId);
        states.finished(txnId);
    }

    @Override
    public void rollback(Xid xid) {
        String txnId = txnId(xid);
        if (txnId.equals(failedLocally)) {
            failedLocally = null;
            states.finished(txnId);
        } else {
            states.rolledBackByCoordinator(txnId);
        }
        shim.abort(txnId);
    }

    @Override
    public void forget(Xid xid) {
        states.finished(txnId(xid));
    }

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

    private void abortObserved(String txnId) {
        shim.abort(txnId);
        states.finished(txnId);
    }

    private static XAException xaError(int errorCode, String message) {
        XAException e = new XAException(message);
        e.errorCode = errorCode;
        return e;
    }
}
