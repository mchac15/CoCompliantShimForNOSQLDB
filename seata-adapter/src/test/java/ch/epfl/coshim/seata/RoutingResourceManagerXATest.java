package ch.epfl.coshim.seata;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ch.epfl.coshim.store.InMemoryKvStore;
import org.apache.seata.core.model.BranchStatus;
import org.apache.seata.core.model.BranchType;
import org.apache.seata.rm.DefaultResourceManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class RoutingResourceManagerXATest {

    private FakeShim shim;
    private ShimResource<String, String> resource;
    private TestRoutingResourceManagerXA rm;

    @BeforeEach
    void setUp() {
        shim = new FakeShim(new InMemoryKvStore<>());
        resource = new ShimResource<>("test-kv", shim);
        rm = new TestRoutingResourceManagerXA();
        rm.registerResource(resource);
    }

    @Test
    void spiMakesRoutingRmTheXaResourceManager() {
        assertTrue(DefaultResourceManager.get().getResourceManager(BranchType.XA) instanceof RoutingResourceManagerXA);
        assertTrue(CoShimSeata.routingResourceManager() instanceof RoutingResourceManagerXA);
    }

    @Test
    void shimResourceIsAnXaResourceAndManaged() {
        assertEquals(BranchType.XA, resource.getBranchType());
        assertEquals("coshim://test-kv", resource.getResourceId());
        assertSame(resource, rm.getManagedResources().get("coshim://test-kv"));
    }

    @Test
    void branchCommitCallsShimCommitWithTxnIdFromApplicationData() throws Exception {
        BranchStatus status = rm.branchCommit(BranchType.XA, "xid", 7, resource.getResourceId(), "txn-1");
        assertEquals(BranchStatus.PhaseTwo_Committed, status);
        assertEquals(java.util.List.of("commit:txn-1"), shim.calls);
    }

    @Test
    void branchRollbackCallsShimAbort() throws Exception {
        BranchStatus status = rm.branchRollback(BranchType.XA, "xid", 7, resource.getResourceId(), "txn-1");
        assertEquals(BranchStatus.PhaseTwo_Rollbacked, status);
        assertEquals(java.util.List.of("abort:txn-1"), shim.calls);
    }

    @Test
    void shimExceptionIsRetryable() throws Exception {
        ShimResource<String, String> broken = new ShimResource<>("broken", new FakeShim(new InMemoryKvStore<>()) {
            @Override
            public void commit(String txnId) {
                throw new IllegalStateException("boom");
            }
        });
        rm.registerResource(broken);
        assertEquals(
                BranchStatus.PhaseTwo_CommitFailed_Retryable,
                rm.branchCommit(BranchType.XA, "xid", 7, broken.getResourceId(), "txn-1"));
    }

    @Test
    void missingTxnIdIsUnretryable() throws Exception {
        assertEquals(
                BranchStatus.PhaseTwo_RollbackFailed_Unretryable,
                rm.branchRollback(BranchType.XA, "xid", 7, resource.getResourceId(), null));
        assertTrue(shim.calls.isEmpty());
    }

    @Test
    void nonShimResourceFallsThroughToStockXa() throws Exception {
        // stock ResourceManagerXA answers this for any resource it does not know
        assertEquals(
                BranchStatus.PhaseTwo_CommitFailed_Unretryable,
                rm.branchCommit(BranchType.XA, "xid", 7, "jdbc:mysql://db/unknown", null));
        assertTrue(shim.calls.isEmpty());
    }
}
