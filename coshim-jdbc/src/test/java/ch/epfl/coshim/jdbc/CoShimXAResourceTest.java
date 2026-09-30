package ch.epfl.coshim.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ch.epfl.coshim.core.NoCcShim;
import ch.epfl.coshim.core.Vote;
import ch.epfl.coshim.store.InMemoryKvStore;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.sql.SQLTransactionRollbackException;
import java.util.List;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class CoShimXAResourceTest {

    record TestXid(String gtrid, String bqual) implements Xid {
        @Override
        public int getFormatId() {
            return 9752;
        }

        @Override
        public byte[] getGlobalTransactionId() {
            return gtrid.getBytes(StandardCharsets.UTF_8);
        }

        @Override
        public byte[] getBranchQualifier() {
            return bqual.getBytes(StandardCharsets.UTF_8);
        }
    }

    private final Xid xid = new TestXid("127.0.0.1:8091:42", "-7");

    private InMemoryKvStore<String, String> store;
    private NoCcShim<String, String> shim;
    private CoShimDataSource<String, String> dataSource;

    @BeforeEach
    void setUp() {
        store = new InMemoryKvStore<>();
        shim = new NoCcShim<>(store);
        dataSource = new CoShimDataSource<>("orders-kv", shim);
    }

    @Test
    void urlIsTheResourceIdentity() throws SQLException {
        assertEquals("jdbc:coshim://orders-kv", dataSource.getConnection().getMetaData().getURL());
    }

    @Test
    void fullCommitPathAppliesWritesOnlyInPhaseTwo() throws Exception {
        CoShimXAConnection xa = dataSource.getXAConnection();
        XAResource res = xa.getXAResource();
        KvSession<String, String> session = KvSession.from(xa.getConnection());

        res.start(xid, XAResource.TMNOFLAGS);
        session.put("x", "1");
        assertEquals("1", session.get("x"));
        res.end(xid, XAResource.TMSUCCESS);
        assertEquals(XAResource.XA_OK, res.prepare(xid));
        assertNull(store.get("x"));

        // phase 2 arrives on a fresh connection, as with Seata's ResourceManagerXA
        dataSource.getXAConnection().getXAResource().commit(xid, false);
        assertEquals("1", store.get("x"));
        assertEquals(0, dataSource.getBranchStates().size());
    }

    @Test
    void phaseTwoRollbackDiscardsWrites() throws Exception {
        XAResource res = dataSource.getXAConnection().getXAResource();
        res.start(xid, XAResource.TMNOFLAGS);
        res.end(xid, XAResource.TMSUCCESS);
        res.prepare(xid);

        XAResource fresh = dataSource.getXAConnection().getXAResource();
        fresh.end(xid, XAResource.TMFAIL);   // what ConnectionProxyXA.xaRollback does first
        fresh.rollback(xid);
        assertEquals(Vote.NO, shim.prepare(CoShimXAResource.txnId(xid)), "txn is gone from the shim");
        assertEquals(0, dataSource.getBranchStates().size());
    }

    @Test
    void localPhaseOneRollbackLeavesNoState() throws Exception {
        CoShimXAConnection xa = dataSource.getXAConnection();
        XAResource res = xa.getXAResource();
        res.start(xid, XAResource.TMNOFLAGS);
        KvSession.<String, String>from(xa.getConnection()).put("x", "1");
        res.end(xid, XAResource.TMFAIL);
        res.rollback(xid);
        assertEquals(0, dataSource.getBranchStates().size());
        assertNull(store.get("x"));
    }

    @Test
    void noVoteIsXaRbRollback() throws Exception {
        CoShimDataSource<String, String> ds = new CoShimDataSource<>("no", new NoCcShim<>(store) {
            @Override
            public Vote prepare(String txnId) {
                abort(txnId);
                return Vote.NO;
            }
        });
        XAResource res = ds.getXAConnection().getXAResource();
        res.start(xid, XAResource.TMNOFLAGS);
        res.end(xid, XAResource.TMSUCCESS);
        XAException e = assertThrows(XAException.class, () -> res.prepare(xid));
        assertEquals(XAException.XA_RBROLLBACK, e.errorCode);
        assertEquals(0, ds.getBranchStates().size());
    }

    @Test
    void coordinatorRollbackBeforeStartRefusesTheStart() throws Exception {
        dataSource.getXAConnection().getXAResource().rollback(xid);   // TC timeout raced ahead of xa start

        CoShimXAConnection xa = dataSource.getXAConnection();
        ((CoShimConnection<?, ?>) xa.getConnection()).requireXaBranch();   // as DataSourceProxyCoShim does in a global txn
        XAException e = assertThrows(XAException.class, () -> xa.getXAResource().start(xid, XAResource.TMNOFLAGS));
        assertEquals(XAException.XA_RBROLLBACK, e.errorCode);
        assertThrows(SQLException.class, () -> KvSession.<String, String>from(xa.getConnection()).put("x", "1"));
        assertEquals(0, dataSource.getBranchStates().size(), "the tombstone is consumed");
    }

    @Test
    void coordinatorRollbackDuringPhaseOneAbortsAndFailsTheBranch() throws Exception {
        CoShimXAConnection xa = dataSource.getXAConnection();
        XAResource res = xa.getXAResource();
        KvSession<String, String> session = KvSession.from(xa.getConnection());
        res.start(xid, XAResource.TMNOFLAGS);
        session.put("x", "1");

        dataSource.getXAConnection().getXAResource().rollback(xid);   // TC rollback on another connection

        assertThrows(SQLTransactionRollbackException.class, () -> session.put("y", "2"));
        XAException e = assertThrows(XAException.class, () -> res.end(xid, XAResource.TMSUCCESS));
        assertEquals(XAException.XA_RBROLLBACK, e.errorCode);
        assertEquals(0, dataSource.getBranchStates().size());
        assertNull(store.get("x"));
    }

    @Test
    void branchesOnDifferentConnectionsAreActiveAtTheSameTime() throws Exception {
        Xid xid2 = new TestXid("127.0.0.1:8091:43", "-8");
        CoShimXAConnection a = dataSource.getXAConnection();
        CoShimXAConnection b = dataSource.getXAConnection();
        a.getXAResource().start(xid, XAResource.TMNOFLAGS);
        b.getXAResource().start(xid2, XAResource.TMNOFLAGS);

        // interleaved requests of two open transactions, each routed by its own connection
        KvSession.<String, String>from(a.getConnection()).put("x", "a");
        KvSession.<String, String>from(b.getConnection()).put("y", "b");
        assertNull(KvSession.<String, String>from(a.getConnection()).get("y"), "b's write is not visible to a");

        for (CoShimXAConnection c : List.of(a, b)) {
            Xid x = c == a ? xid : xid2;
            c.getXAResource().end(x, XAResource.TMSUCCESS);
            c.getXAResource().prepare(x);
            c.getXAResource().commit(x, false);
        }
        assertEquals("a", store.get("x"));
        assertEquals("b", store.get("y"));
    }

    @Test
    void localRollbackOnTheStartingResourceLeavesNoState() throws Exception {
        // ConnectionProxyXA's branch-execution timeout in close(): xa end(TMSUCCESS), then xa rollback on
        // the same connection, without end(TMFAIL). It is local, not a coordinator rollback.
        XAResource res = dataSource.getXAConnection().getXAResource();
        res.start(xid, XAResource.TMNOFLAGS);
        res.end(xid, XAResource.TMSUCCESS);
        res.rollback(xid);
        assertEquals(0, dataSource.getBranchStates().size(), "no tombstone for a local rollback");
    }

    @Test
    void coordinatorRollbackLandingDuringPrepareFailsThePrepare() throws Exception {
        CoShimDataSource<String, String>[] ds = new CoShimDataSource[1];
        ds[0] = new CoShimDataSource<>("racy", new NoCcShim<>(store) {
            @Override
            public Vote prepare(String txnId) {
                Vote vote = super.prepare(txnId);                       // shim votes YES ...
                ((CoShimXAResource) ds[0].getXAConnection().getXAResource()).rollback(xid); // ... then the TC's rollback lands
                return vote;
            }
        });
        XAResource res = ds[0].getXAConnection().getXAResource();
        res.start(xid, XAResource.TMNOFLAGS);
        res.end(xid, XAResource.TMSUCCESS);
        XAException e = assertThrows(XAException.class, () -> res.prepare(xid));
        assertEquals(XAException.XA_RBROLLBACK, e.errorCode);
        assertEquals(0, ds[0].getBranchStates().size(), "no stale entry");
    }

    @Test
    void localAutocommitRequestsAreTheirOwnTransactions() throws Exception {
        CoShimConnection<String, String> c = dataSource.getConnection();
        c.put("x", "1");
        assertEquals("1", store.get("x"), "committed right away");
        assertEquals("1", c.get("x"));
    }

    @Test
    void localTransactionCommitsOnCommitAndDiscardsOnRollback() throws Exception {
        CoShimConnection<String, String> c = dataSource.getConnection();
        c.setAutoCommit(false);
        c.put("x", "1");
        assertEquals("1", c.get("x"));
        assertNull(store.get("x"));
        c.commit();
        assertEquals("1", store.get("x"));

        c.put("y", "2");
        c.rollback();
        assertNull(store.get("y"));

        c.put("z", "3");
        c.setAutoCommit(true);   // JDBC: commits the transaction in progress
        assertEquals("3", store.get("z"));
    }

    @Test
    void xaStartIsRefusedWhileALocalTransactionIsOpen() throws Exception {
        CoShimXAConnection xa = dataSource.getXAConnection();
        xa.getConnection().setAutoCommit(false);
        KvSession.<String, String>from(xa.getConnection()).put("x", "1");
        XAException e = assertThrows(XAException.class, () -> xa.getXAResource().start(xid, XAResource.TMNOFLAGS));
        assertEquals(XAException.XAER_OUTSIDE, e.errorCode);
    }

    @Test
    void connectionRequiringABranchRefusesLocalRequests() throws Exception {
        CoShimConnection<String, String> c = dataSource.getConnection();
        c.requireXaBranch();
        assertThrows(SQLException.class, () -> c.put("x", "1"));
        assertNull(store.get("x"));
    }

    @Test
    void txnIdIsStableAcrossConnections() {
        assertTrue(CoShimXAResource.txnId(xid).endsWith("127.0.0.1:8091:42:-7"));
        assertEquals(CoShimXAResource.txnId(xid), CoShimXAResource.txnId(new TestXid("127.0.0.1:8091:42", "-7")));
    }
}
