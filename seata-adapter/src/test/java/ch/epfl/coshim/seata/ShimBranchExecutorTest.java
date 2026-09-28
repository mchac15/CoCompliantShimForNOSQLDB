package ch.epfl.coshim.seata;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ch.epfl.coshim.core.TxnAbortedException;
import ch.epfl.coshim.store.InMemoryKvStore;
import java.util.List;
import org.apache.seata.core.context.RootContext;
import org.apache.seata.core.model.BranchStatus;
import org.apache.seata.core.model.BranchType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ShimBranchExecutorTest {

    private static final String XID = "127.0.0.1:8091:42";

    private InMemoryKvStore<String, String> store;
    private FakeShim shim;
    private ShimResource<String, String> resource;
    private TestRoutingResourceManagerXA rm;
    private ShimBranchExecutor executor;

    @BeforeEach
    void setUp() {
        store = new InMemoryKvStore<>();
        shim = new FakeShim(store);
        resource = new ShimResource<>("test-kv", shim);
        rm = new TestRoutingResourceManagerXA();
        rm.registerResource(resource);
        executor = new ShimBranchExecutor(rm);
        RootContext.bind(XID);
    }

    @AfterEach
    void tearDown() {
        RootContext.unbind();
    }

    @Test
    void successfulBranchRegistersExecutesPreparesAndCommitsInPhaseTwo() throws Exception {
        executor.run(resource, ctx -> ctx.put("x", "1"));

        TestRoutingResourceManagerXA.Registration reg = rm.registrations.get(0);
        assertEquals(resource.getResourceId(), reg.resourceId());
        assertEquals(XID, reg.xid());
        String txnId = reg.applicationData();
        assertTrue(txnId.startsWith(XID + ":"));
        assertEquals(List.of("execute:" + txnId, "prepare:" + txnId), shim.calls);
        assertTrue(rm.reports.isEmpty());
        assertNull(store.get("x"), "nothing reaches the store before phase 2");

        // phase 2, as the TC would send it after the global commit
        assertEquals(
                BranchStatus.PhaseTwo_Committed,
                rm.branchCommit(BranchType.XA, XID, reg.branchId(), reg.resourceId(), reg.applicationData()));
        assertEquals("1", store.get("x"));
    }

    @Test
    void failedExecuteReportsPhaseOneFailedAndThrows() {
        assertThrows(ShimBranchException.class, () -> executor.run(resource, ctx -> {
            throw new TxnAbortedException("lock timeout");
        }));
        assertEquals(List.of(new TestRoutingResourceManagerXA.Report(1, BranchStatus.PhaseOne_Failed)), rm.reports);
        assertTrue(shim.txns.isEmpty());
    }

    @Test
    void noVoteReportsPhaseOneFailedAndThrows() {
        shim.voteNo = true;
        assertThrows(ShimBranchException.class, () -> executor.run(resource, ctx -> ctx.put("x", "1")));
        assertEquals(List.of(new TestRoutingResourceManagerXA.Report(1, BranchStatus.PhaseOne_Failed)), rm.reports);
        assertTrue(shim.txns.isEmpty());
    }

    @Test
    void tcRollbackBeforeShimKnowsTheTxnDoesNotLeaveAPreparedTxn() {
        // the TC rolls the branch back (e.g. global timeout) after branchRegister but before execute
        // registered the txn: the shim's abort is a no-op, so the executor must abort afterwards
        shim.beforeRegister = () -> {
            TestRoutingResourceManagerXA.Registration reg = rm.registrations.get(0);
            try {
                rm.branchRollback(BranchType.XA, XID, reg.branchId(), reg.resourceId(), reg.applicationData());
            } catch (Exception e) {
                throw new AssertionError(e);
            }
        };

        assertThrows(ShimBranchException.class, () -> executor.run(resource, ctx -> ctx.put("x", "1")));
        assertTrue(shim.txns.isEmpty(), "no txn may stay registered (and hold locks) after a global rollback");
        assertTrue(rm.reports.isEmpty());
    }

    @Test
    void requiresAGlobalTransaction() {
        RootContext.unbind();
        assertThrows(IllegalStateException.class, () -> executor.run(resource, ctx -> {}));
        assertTrue(rm.registrations.isEmpty());
    }
}
