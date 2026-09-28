package ch.epfl.coshim.seata;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ch.epfl.coshim.core.NoCcShim;
import ch.epfl.coshim.core.Vote;
import ch.epfl.coshim.jdbc.CoShimDataSource;
import ch.epfl.coshim.jdbc.KvSession;
import ch.epfl.coshim.store.InMemoryKvStore;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import org.apache.seata.core.context.RootContext;
import org.apache.seata.core.model.BranchStatus;
import org.apache.seata.core.model.BranchType;
import org.apache.seata.rm.DefaultResourceManager;
import org.apache.seata.rm.datasource.xa.ConnectionProxyXA;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * The coshim data source through Seata's real XA classes: DataSourceProxyXA / ConnectionProxyXA for
 * phase 1 and ResourceManagerXA for phase 2, exactly as for MySQL/PG. Only the TC round trips are
 * stubbed (TestResourceManagerXA).
 */
class DataSourceProxyCoShimTest {

    private static final String XID = "127.0.0.1:8091:42";
    private static final String RESOURCE_ID = "jdbc:coshim://orders-kv";

    private InMemoryKvStore<String, String> store;
    private TestResourceManagerXA rm;
    private DataSourceProxyCoShim proxy;

    @BeforeEach
    void setUp() {
        store = new InMemoryKvStore<>();
        rm = new TestResourceManagerXA();
        DefaultResourceManager.get();   // its lazy SPI init would otherwise overwrite the mock
        DefaultResourceManager.mockResourceManager(BranchType.XA, rm);
        proxy = newProxy(new NoCcShim<>(store));
    }

    private DataSourceProxyCoShim newProxy(NoCcShim<String, String> shim) {
        return new DataSourceProxyCoShim(new CoShimDataSource<>("orders-kv", shim));
    }

    @AfterEach
    void tearDown() {
        RootContext.unbind();
    }

    /** Phase 1 as an application does it on any XA data source. */
    private TestResourceManagerXA.Registration runBranch(String key, String value) throws SQLException {
        RootContext.bind(XID);
        try (Connection c = proxy.getConnection()) {
            assertTrue(c instanceof ConnectionProxyXA, "Seata's stock XA connection proxy");
            c.setAutoCommit(false);
            KvSession<String, String> kv = KvSession.from(c);
            kv.put(key, value);
            assertEquals(value, kv.get(key));
            c.commit();
        } finally {
            RootContext.unbind();
        }
        return rm.registrations.get(rm.registrations.size() - 1);
    }

    @Test
    void isAnXaDataSourceResourceLikeMysqlOrPg() {
        assertEquals(RESOURCE_ID, proxy.getResourceId());
        assertEquals("coshim", proxy.getDbType());
        assertEquals(BranchType.XA, proxy.getBranchType());
        assertTrue(rm.getManagedResources().containsKey(RESOURCE_ID));
    }

    @Test
    void commitPathThroughSeataXa() throws Exception {
        TestResourceManagerXA.Registration reg = runBranch("x", "1");
        assertEquals(RESOURCE_ID, reg.resourceId());
        assertEquals(XID, reg.xid());
        assertTrue(rm.reports.isEmpty(), "xa prepare returned XA_OK, nothing to report");
        assertNull(store.get("x"), "nothing reaches the store before phase 2");

        // phase 2: what the TC sends after the global commit, handled by stock ResourceManagerXA
        assertEquals(
                BranchStatus.PhaseTwo_Committed,
                rm.branchCommit(BranchType.XA, XID, reg.branchId(), RESOURCE_ID, null));
        assertEquals("1", store.get("x"));
    }

    @Test
    void phaseTwoRollbackThroughSeataXa() throws Exception {
        TestResourceManagerXA.Registration reg = runBranch("x", "1");
        assertEquals(
                BranchStatus.PhaseTwo_Rollbacked,
                rm.branchRollback(BranchType.XA, XID, reg.branchId(), RESOURCE_ID, null));
        assertNull(store.get("x"));
    }

    @Test
    void applicationRollbackReportsPhaseOneFailed() throws Exception {
        RootContext.bind(XID);
        try (Connection c = proxy.getConnection()) {
            c.setAutoCommit(false);
            KvSession.<String, String>from(c).put("x", "1");
            c.rollback();
        }
        assertEquals(List.of(new TestResourceManagerXA.Report(1, BranchStatus.PhaseOne_Failed)), rm.reports);
        assertNull(store.get("x"));
    }

    @Test
    void noVoteFailsCloseAndReportsPhaseOneFailed() throws Exception {
        NoCcShim<String, String> votesNo = new NoCcShim<>(store) {
            @Override
            public Vote prepare(String txnId) {
                abort(txnId);
                return Vote.NO;
            }
        };
        proxy = newProxy(votesNo);
        RootContext.bind(XID);
        Connection c = proxy.getConnection();
        c.setAutoCommit(false);
        KvSession.<String, String>from(c).put("x", "1");
        assertThrows(SQLException.class, c::close, "xa prepare threw XA_RBROLLBACK");
        assertEquals(List.of(new TestResourceManagerXA.Report(1, BranchStatus.PhaseOne_Failed)), rm.reports);
    }

    @Test
    void coordinatorRollbackRacingXaStartFailsTheBranch() throws Exception {
        // the TC rolls the branch back (e.g. global timeout) right after registering it, before xa start
        rm.afterRegister = reg -> rm.branchRollback(BranchType.XA, reg.xid(), reg.branchId(), reg.resourceId(), null);

        RootContext.bind(XID);
        try (Connection c = proxy.getConnection()) {
            assertThrows(SQLException.class, () -> c.setAutoCommit(false), "xa start refused (XA_RBROLLBACK)");
            assertThrows(SQLException.class, () -> KvSession.<String, String>from(c).put("x", "1"));
        }
        assertNull(store.get("x"));
    }

    @Test
    void outsideAGlobalTransactionTheConnectionIsNotEnlisted() throws Exception {
        try (Connection c = proxy.getConnection()) {
            assertFalse(c instanceof ConnectionProxyXA);
            assertThrows(SQLException.class, () -> KvSession.<String, String>from(c).get("x"));
        }
    }
}
