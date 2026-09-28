package ch.epfl.coshim.seata;

import ch.epfl.coshim.core.CoShim;
import ch.epfl.coshim.core.Outcome;
import ch.epfl.coshim.core.TxnCode;
import ch.epfl.coshim.core.Vote;
import java.util.Objects;
import java.util.UUID;
import org.apache.seata.core.context.RootContext;
import org.apache.seata.core.exception.TransactionException;
import org.apache.seata.core.model.BranchStatus;
import org.apache.seata.core.model.BranchType;
import org.apache.seata.core.model.ResourceManager;
import org.apache.seata.rm.DefaultResourceManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Runs one shim subtransaction as an XA branch of the current Seata global transaction. It is
 * phase 1, mirroring {@code ConnectionProxyXA} (register, run, then prepare locally before the TM's
 * global commit):
 *
 * <ol>
 *   <li>register an XA branch with the TC (no lock keys), passing the shim txn id as applicationData
 *       so phase 2 can find it;</li>
 *   <li>{@link CoShim#execute} the code;</li>
 *   <li>{@link CoShim#prepare}, which blocks until every predecessor has resolved, then votes.</li>
 * </ol>
 *
 * Phase 2 (commit/abort) comes later from the TC through {@link RoutingResourceManagerXA}. The
 * global transaction timeout plays the coordinator's vote timeout (pseudo.txt, assumption 4): a TC
 * rollback aborts the txn, which releases a blocked prepare with NO.
 */
public class ShimBranchExecutor {

    private static final Logger LOGGER = LoggerFactory.getLogger(ShimBranchExecutor.class);

    private final ResourceManager resourceManager;

    public ShimBranchExecutor() {
        this(DefaultResourceManager.get());
    }

    public ShimBranchExecutor(ResourceManager resourceManager) {
        this.resourceManager = Objects.requireNonNull(resourceManager);
    }

    /**
     * @throws IllegalStateException if the calling thread is not in a global transaction
     * @throws ShimBranchException if phase 1 failed; the global transaction must roll back
     */
    public <K, V> void run(ShimResource<K, V> resource, TxnCode<K, V> code) {
        String xid = RootContext.getXID();
        if (xid == null) {
            throw new IllegalStateException("No global transaction bound to this thread (RootContext.getXID())");
        }
        CoShim<K, V> shim = resource.getShim();
        String txnId = xid + ":" + UUID.randomUUID();

        // before branchRegister: from then on the TC may send a rollback at any time
        resource.beginPhaseOne(txnId);
        try {
            long branchId;
            try {
                branchId = resourceManager.branchRegister(
                        BranchType.XA, resource.getResourceId(), null, xid, txnId, null);
            } catch (TransactionException e) {
                throw new ShimBranchException("Failed to register shim branch on " + resource, e);
            }

            Outcome outcome;
            Vote vote;
            try {
                outcome = shim.execute(txnId, code);
                vote = outcome == Outcome.SUCCEEDED && !resource.isRollbackRequested(txnId)
                        ? shim.prepare(txnId)
                        : Vote.NO;
            } catch (RuntimeException e) {
                shim.abort(txnId);
                reportPhaseOneFailed(xid, branchId);
                throw new ShimBranchException("Shim branch " + txnId + " threw", e);
            }

            if (resource.isRollbackRequested(txnId)) {
                // the TC rolled back while execute/prepare ran; its abort may have come before the shim
                // registered the txn, so abort here (idempotent). No report: the branch is already done
                shim.abort(txnId);
                throw new ShimBranchException("Global transaction " + xid + " rolled back during phase 1");
            }
            if (outcome == Outcome.FAILED || vote == Vote.NO) {
                // the shim already aborted the txn and released its locks (execute/prepare do that)
                reportPhaseOneFailed(xid, branchId);
                throw new ShimBranchException("Shim branch " + txnId + (outcome == Outcome.FAILED
                        ? " failed to execute" : " voted NO"));
            }
        } finally {
            resource.endPhaseOne(txnId);
        }
    }

    private void reportPhaseOneFailed(String xid, long branchId) {
        try {
            resourceManager.branchReport(BranchType.XA, xid, branchId, BranchStatus.PhaseOne_Failed, null);
        } catch (TransactionException e) {
            // the global rollback that follows still reaches the branch; abort is a no-op by then
            LOGGER.warn("Failed to report PhaseOne_Failed for {}:{}", xid, branchId, e);
        }
    }
}
