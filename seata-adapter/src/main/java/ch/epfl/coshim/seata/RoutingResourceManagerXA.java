package ch.epfl.coshim.seata;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.seata.common.loader.LoadLevel;
import org.apache.seata.core.exception.TransactionException;
import org.apache.seata.core.model.BranchStatus;
import org.apache.seata.core.model.BranchType;
import org.apache.seata.core.model.Resource;
import org.apache.seata.core.rpc.netty.RmNettyRemotingClient;
import org.apache.seata.rm.datasource.xa.ResourceManagerXA;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The XA resource manager of the application: {@link ShimResource}s are handled here, every other
 * resource (stock XA and Sonata data sources) goes to {@link ResourceManagerXA} unchanged. This is
 * what lets one global transaction mix shim branches with MySQL/PostgreSQL XA branches.
 *
 * <p>Seata keeps one RM per {@link BranchType}. The SPI loader sorts by {@link LoadLevel#order()}
 * and the last one wins, so the order here must stay above the stock RM's default (0).
 *
 * <p>Shim resources are kept out of {@code dataSourceCache}: the XA two-phase-hold checker of the
 * superclass casts every entry of that cache to {@code BaseDataSourceResource}.
 *
 * <p>Phase 2 maps the TC's decision onto the shim: branchCommit calls {@code commit}, branchRollback
 * calls {@code abort}. The shim txn id is the branch's applicationData, set by
 * {@link ShimBranchExecutor} at registration.
 */
@LoadLevel(name = "CoShimXA", order = 100)
public class RoutingResourceManagerXA extends ResourceManagerXA {

    private static final Logger LOGGER = LoggerFactory.getLogger(RoutingResourceManagerXA.class);

    private final Map<String, ShimResource<?, ?>> shimResources = new ConcurrentHashMap<>();

    @Override
    public void registerResource(Resource resource) {
        if (resource instanceof ShimResource) {
            shimResources.put(resource.getResourceId(), (ShimResource<?, ?>) resource);
            registerWithTc(resource);
        } else {
            super.registerResource(resource);
        }
    }

    /** Announces the resource id to the TC so phase-2 requests for it are routed to this client. */
    protected void registerWithTc(Resource resource) {
        RmNettyRemotingClient.getInstance().registerResource(resource.getResourceGroupId(), resource.getResourceId());
    }

    @Override
    public Map<String, Resource> getManagedResources() {
        // RmNettyRemotingClient re-registers these ids with the TC on reconnect
        Map<String, Resource> all = new HashMap<>(super.getManagedResources());
        all.putAll(shimResources);
        return all;
    }

    @Override
    public BranchStatus branchCommit(
            BranchType branchType, String xid, long branchId, String resourceId, String applicationData)
            throws TransactionException {
        ShimResource<?, ?> resource = shimResources.get(resourceId);
        if (resource == null) {
            return super.branchCommit(branchType, xid, branchId, resourceId, applicationData);
        }
        return finishShimBranch(true, resource, xid, branchId, applicationData);
    }

    @Override
    public BranchStatus branchRollback(
            BranchType branchType, String xid, long branchId, String resourceId, String applicationData)
            throws TransactionException {
        ShimResource<?, ?> resource = shimResources.get(resourceId);
        if (resource == null) {
            return super.branchRollback(branchType, xid, branchId, resourceId, applicationData);
        }
        return finishShimBranch(false, resource, xid, branchId, applicationData);
    }

    private BranchStatus finishShimBranch(
            boolean commit, ShimResource<?, ?> resource, String xid, long branchId, String txnId) {
        if (txnId == null || txnId.isEmpty()) {
            LOGGER.error("Shim branch {}:{} on {} has no txn id in its applicationData", xid, branchId, resource);
            return commit ? BranchStatus.PhaseTwo_CommitFailed_Unretryable
                    : BranchStatus.PhaseTwo_RollbackFailed_Unretryable;
        }
        try {
            // commit/abort are idempotent in the shim (unknown id: no-op), so a TC retry is safe
            if (commit) {
                resource.getShim().commit(txnId);
                return BranchStatus.PhaseTwo_Committed;
            }
            resource.markRollbackIfInPhaseOne(txnId);
            resource.getShim().abort(txnId);
            return BranchStatus.PhaseTwo_Rollbacked;
        } catch (RuntimeException e) {
            LOGGER.error("Shim branch {}:{} ({}) {} failed", xid, branchId, txnId, commit ? "commit" : "rollback", e);
            return commit ? BranchStatus.PhaseTwo_CommitFailed_Retryable
                    : BranchStatus.PhaseTwo_RollbackFailed_Retryable;
        }
    }
}
