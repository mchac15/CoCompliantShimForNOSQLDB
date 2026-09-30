package ch.epfl.coshim.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ch.epfl.coshim.core.NoCcShim;
import ch.epfl.coshim.core.Vote;
import ch.epfl.coshim.store.FlatKvStore;
import ch.epfl.coshim.store.InMemoryKvStore;
import ch.epfl.coshim.store.TableKey;
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

    private static final String T = "orders";

    private final Xid xid = new TestXid("127.0.0.1:8091:42", "-7");

    private InMemoryKvStore<TableKey<String>, String> store;
    private NoCcShim<TableKey<String>, String> shim;
    private CoShimDataSource<String, String> dataSource;

    @BeforeEach
    void setUp() {
        store = new InMemoryKvStore<>();
        shim = new NoCcShim<>(store);
        dataSource = new CoShimDataSource<>("localhost:7000/shop", shim);
    }

    private String stored(String table, String key) {
        return store.get(new TableKey<>(table, key));
    }

    @Test
    void urlIsTheResourceIdentity() throws SQLException {
        assertEquals("jdbc:coshim://localhost:7000/shop", dataSource.getConnection().getMetaData().getURL());
    }

    @Test
    void fullCommitPathAppliesWritesOnlyInPhaseTwo() throws Exception {
        CoShimXAConnection xa = dataSource.getXAConnection();
        XAResource res = xa.getXAResource();
        KvSession<String, String> session = KvSession.from(xa.getConnection());

        res.start(xid, XAResource.TMNOFLAGS);
        session.put(T, "x", "1");
        assertEquals("1", session.get(T, "x"));
        res.end(xid, XAResource.TMSUCCESS);
        assertEquals(XAResource.XA_OK, res.prepare(xid));
        assertNull(stored(T, "x"));

        // phase 2 arrives on a fresh connection, as with Seata's ResourceManagerXA
        dataSource.getXAConnection().getXAResource().commit(xid, false);
        assertEquals("1", stored(T, "x"));
        assertEquals(0, shim.size());
    }

    @Test
    void tablesOfTheDatabaseAreSeparateKeySpaces() throws Exception {
        CoShimConnection<String, String> c = dataSource.getConnection();
        c.put("orders", "1", "an order");
        c.put("stock", "1", "a stock level");
        assertEquals("an order", c.get("orders", "1"));
        assertEquals("a stock level", c.get("stock", "1"));
    }

    @Test
    void tablesAreOptionalPlainKeysWorkOnAStoreWithoutTables() throws Exception {
        InMemoryKvStore<String, String> plainStore = new InMemoryKvStore<>();   // a store with no table notion
        CoShimDataSource<String, String> flat =
                new CoShimDataSource<>("localhost:7000/flat", new NoCcShim<>(new FlatKvStore<>(plainStore)));
        CoShimConnection<String, String> c = flat.getConnection();
        c.put("x", "1");
        assertEquals("1", c.get("x"));
        assertEquals("1", plainStore.get("x"), "stored under the plain key");
    }

    @Test
    void phaseTwoRollbackDiscardsWrites() throws Exception {
        CoShimXAConnection xa = dataSource.getXAConnection();
        XAResource res = xa.getXAResource();
        res.start(xid, XAResource.TMNOFLAGS);
        KvSession.<String, String>from(xa.getConnection()).put(T, "x", "1");
        res.end(xid, XAResource.TMSUCCESS);
        res.prepare(xid);

        XAResource fresh = dataSource.getXAConnection().getXAResource();
        fresh.end(xid, XAResource.TMFAIL);   // what ConnectionProxyXA.xaRollback does first
        fresh.rollback(xid);
        assertNull(stored(T, "x"));
        assertEquals(0, shim.size(), "aborting a known txn leaves no tombstone");
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
    }

    @Test
    void coordinatorRollbackBeforeStartRefusesTheStart() throws Exception {
        dataSource.getXAConnection().getXAResource().rollback(xid);   // TC timeout raced ahead of xa start
        assertEquals(1, shim.size(), "the shim keeps a tombstone");

        CoShimXAConnection xa = dataSource.getXAConnection();
        ((CoShimConnection<?, ?>) xa.getConnection()).requireXaBranch();   // as DataSourceProxyCoShim does in a global txn
        XAException e = assertThrows(XAException.class, () -> xa.getXAResource().start(xid, XAResource.TMNOFLAGS));
        assertEquals(XAException.XA_RBROLLBACK, e.errorCode);
        assertThrows(SQLException.class, () -> KvSession.<String, String>from(xa.getConnection()).put(T, "x", "1"));
    }

    @Test
    void coordinatorRollbackDuringPhaseOneFailsEveryLaterStep() throws Exception {
        CoShimXAConnection xa = dataSource.getXAConnection();
        XAResource res = xa.getXAResource();
        KvSession<String, String> session = KvSession.from(xa.getConnection());
        res.start(xid, XAResource.TMNOFLAGS);
        session.put(T, "x", "1");

        dataSource.getXAConnection().getXAResource().rollback(xid);   // TC rollback on another connection

        assertThrows(SQLTransactionRollbackException.class, () -> session.put(T, "y", "2"));
        XAException e = assertThrows(XAException.class, () -> res.end(xid, XAResource.TMSUCCESS));
        assertEquals(XAException.XA_RBROLLBACK, e.errorCode);
        assertThrows(XAException.class, () -> res.prepare(xid));
        assertNull(stored(T, "x"));
    }

    @Test
    void coordinatorRollbackLandingAfterTheYesVoteStillAbortsTheTxn() throws Exception {
        XAResource res = dataSource.getXAConnection().getXAResource();
        res.start(xid, XAResource.TMNOFLAGS);
        res.end(xid, XAResource.TMSUCCESS);
        assertEquals(XAResource.XA_OK, res.prepare(xid));

        dataSource.getXAConnection().getXAResource().rollback(xid);
        dataSource.getXAConnection().getXAResource().commit(xid, false);   // a late/duplicate commit is a no-op
        assertEquals(0, shim.size());
    }

    @Test
    void branchesOnDifferentConnectionsAreActiveAtTheSameTime() throws Exception {
        Xid xid2 = new TestXid("127.0.0.1:8091:43", "-8");
        CoShimXAConnection a = dataSource.getXAConnection();
        CoShimXAConnection b = dataSource.getXAConnection();
        a.getXAResource().start(xid, XAResource.TMNOFLAGS);
        b.getXAResource().start(xid2, XAResource.TMNOFLAGS);

        // interleaved requests of two open transactions, each routed by its own connection
        KvSession.<String, String>from(a.getConnection()).put(T, "x", "a");
        KvSession.<String, String>from(b.getConnection()).put(T, "y", "b");
        assertNull(KvSession.<String, String>from(a.getConnection()).get(T, "y"), "b's write is not visible to a");

        for (CoShimXAConnection c : List.of(a, b)) {
            Xid x = c == a ? xid : xid2;
            c.getXAResource().end(x, XAResource.TMSUCCESS);
            c.getXAResource().prepare(x);
            c.getXAResource().commit(x, false);
        }
        assertEquals("a", stored(T, "x"));
        assertEquals("b", stored(T, "y"));
    }

    @Test
    void localAutocommitRequestsAreTheirOwnTransactions() throws Exception {
        CoShimConnection<String, String> c = dataSource.getConnection();
        c.put(T, "x", "1");
        assertEquals("1", stored(T, "x"), "committed right away");
        assertEquals("1", c.get(T, "x"));
    }

    @Test
    void localTransactionCommitsOnCommitAndDiscardsOnRollback() throws Exception {
        CoShimConnection<String, String> c = dataSource.getConnection();
        c.setAutoCommit(false);
        c.put(T, "x", "1");
        assertEquals("1", c.get(T, "x"));
        assertNull(stored(T, "x"));
        c.commit();
        assertEquals("1", stored(T, "x"));

        c.put(T, "y", "2");
        c.rollback();
        assertNull(stored(T, "y"));

        c.put(T, "z", "3");
        c.setAutoCommit(true);   // JDBC: commits the transaction in progress
        assertEquals("3", stored(T, "z"));
    }

    @Test
    void xaStartIsRefusedWhileALocalTransactionIsOpen() throws Exception {
        CoShimXAConnection xa = dataSource.getXAConnection();
        xa.getConnection().setAutoCommit(false);
        KvSession.<String, String>from(xa.getConnection()).put(T, "x", "1");
        XAException e = assertThrows(XAException.class, () -> xa.getXAResource().start(xid, XAResource.TMNOFLAGS));
        assertEquals(XAException.XAER_OUTSIDE, e.errorCode);
    }

    @Test
    void connectionRequiringABranchRefusesLocalRequests() throws Exception {
        CoShimConnection<String, String> c = dataSource.getConnection();
        c.requireXaBranch();
        assertThrows(SQLException.class, () -> c.put(T, "x", "1"));
        assertNull(stored(T, "x"));
    }

    @Test
    void txnIdIsStableAcrossConnections() {
        assertTrue(CoShimXAResource.txnId(xid).endsWith("127.0.0.1:8091:42:-7"));
        assertEquals(CoShimXAResource.txnId(xid), CoShimXAResource.txnId(new TestXid("127.0.0.1:8091:42", "-7")));
    }
}
