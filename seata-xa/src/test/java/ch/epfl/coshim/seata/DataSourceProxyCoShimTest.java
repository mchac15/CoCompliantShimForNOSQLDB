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
import ch.epfl.coshim.store.TableKey;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
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
    private static final String T = "orders";

    private InMemoryKvStore<TableKey<String>, String> store;

    private String stored(String key) {
        return store.get(new TableKey<>(T, key));
    }
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

    private DataSourceProxyCoShim newProxy(NoCcShim<TableKey<String>, String> shim) {
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
            kv.put(T, key, value);
            assertEquals(value, kv.get(T, key));
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
        assertNull(stored("x"), "nothing reaches the store before phase 2");

        // phase 2: what the TC sends after the global commit, handled by stock ResourceManagerXA
        assertEquals(
                BranchStatus.PhaseTwo_Committed,
                rm.branchCommit(BranchType.XA, XID, reg.branchId(), RESOURCE_ID, null));
        assertEquals("1", stored("x"));
    }

    @Test
    void phaseTwoRollbackThroughSeataXa() throws Exception {
        TestResourceManagerXA.Registration reg = runBranch("x", "1");
        assertEquals(
                BranchStatus.PhaseTwo_Rollbacked,
                rm.branchRollback(BranchType.XA, XID, reg.branchId(), RESOURCE_ID, null));
        assertNull(stored("x"));
    }

    @Test
    void applicationRollbackReportsPhaseOneFailed() throws Exception {
        RootContext.bind(XID);
        try (Connection c = proxy.getConnection()) {
            c.setAutoCommit(false);
            KvSession.<String, String>from(c).put(T, "x", "1");
            c.rollback();
        }
        assertEquals(List.of(new TestResourceManagerXA.Report(1, BranchStatus.PhaseOne_Failed)), rm.reports);
        assertNull(stored("x"));
    }

    @Test
    void noVoteFailsCloseAndReportsPhaseOneFailed() throws Exception {
        NoCcShim<TableKey<String>, String> votesNo = new NoCcShim<>(store) {
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
        KvSession.<String, String>from(c).put(T, "x", "1");
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
            assertThrows(SQLException.class, () -> KvSession.<String, String>from(c).put(T, "x", "1"));
        }
        assertNull(stored("x"));
    }

    @Test
    void manyGlobalTransactionsRunConcurrentlyEachOnItsOwnConnection() throws Exception {
        int n = 8;
        CyclicBarrier allInPhaseOne = new CyclicBarrier(n);
        ExecutorService pool = Executors.newFixedThreadPool(n);
        try {
            List<Future<?>> futures = new ArrayList<>();
            for (int i = 0; i < n; i++) {
                int id = i;
                futures.add(pool.submit(() -> {
                    RootContext.bind("127.0.0.1:8091:" + (100 + id));   // thread-local, like in an app
                    try (Connection c = proxy.getConnection()) {
                        c.setAutoCommit(false);
                        KvSession<String, String> kv = KvSession.from(c);
                        kv.put(T, "k" + id, "v" + id);
                        // every branch is open at the same time: nothing serializes transactions
                        allInPhaseOne.await(10, TimeUnit.SECONDS);
                        assertEquals("v" + id, kv.get(T, "k" + id));
                        c.commit();
                    } finally {
                        RootContext.unbind();
                    }
                    return null;
                }));
            }
            for (Future<?> f : futures) {
                f.get(30, TimeUnit.SECONDS);
            }
        } finally {
            pool.shutdownNow();
        }

        assertEquals(n, rm.registrations.size());
        for (TestResourceManagerXA.Registration reg : rm.registrations) {
            assertEquals(
                    BranchStatus.PhaseTwo_Committed,
                    rm.branchCommit(BranchType.XA, reg.xid(), reg.branchId(), RESOURCE_ID, null));
        }
        for (int i = 0; i < n; i++) {
            assertEquals("v" + i, stored("k" + i));
        }
    }

    @Test
    void outsideAGlobalTransactionTheConnectionRunsLocalTransactions() throws Exception {
        try (Connection c = proxy.getConnection()) {
            assertFalse(c instanceof ConnectionProxyXA, "the raw connection, as for MySQL/PG");
            KvSession.<String, String>from(c).put(T, "x", "1");   // autocommit
        }
        assertEquals("1", stored("x"));
        assertTrue(rm.registrations.isEmpty(), "no branch");
    }

    @Test
    void insideAGlobalTransactionRequestsBeforeEnlistmentAreRefused() throws Exception {
        RootContext.bind(XID);
        try (Connection c = proxy.getConnection()) {
            // setAutoCommit(false) forgotten: must not run as a local autocommit write outside the global txn
            assertThrows(SQLException.class, () -> KvSession.<String, String>from(c).put(T, "x", "1"));
        }
        assertNull(stored("x"));
    }
}
