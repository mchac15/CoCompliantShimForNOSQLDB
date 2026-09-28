package ch.epfl.coshim.seata;

import ch.epfl.coshim.core.CoShim;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.seata.core.model.BranchType;
import org.apache.seata.core.model.Resource;

/**
 * A {@link CoShim} seen by Seata as an XA resource. Its branches are ordinary XA branches for the
 * TC (no lock keys, so Seata's global locks stay out of the way), routed on the RM side by
 * {@link RoutingResourceManagerXA}.
 *
 * <p>Also tracks the branches whose phase 1 is still running in {@link ShimBranchExecutor}, so that
 * a TC rollback arriving before the shim knows the txn is not lost (see
 * {@link #markRollbackIfInPhaseOne}).
 */
public class ShimResource<K, V> implements Resource {

    public static final String RESOURCE_ID_PREFIX = "coshim://";
    private static final String DEFAULT_GROUP_ID = "DEFAULT";

    private final String resourceId;
    private final CoShim<K, V> shim;

    /** txnId -> rollback requested by the TC while phase 1 was still running. */
    private final ConcurrentMap<String, Boolean> phaseOne = new ConcurrentHashMap<>();

    /**
     * @param name unique name of this shim among the application's resources, e.g. "orders-kv"
     */
    public ShimResource(String name, CoShim<K, V> shim) {
        this.resourceId = RESOURCE_ID_PREFIX + Objects.requireNonNull(name);
        this.shim = Objects.requireNonNull(shim);
    }

    public CoShim<K, V> getShim() {
        return shim;
    }

    @Override
    public String getResourceGroupId() {
        return DEFAULT_GROUP_ID;
    }

    @Override
    public String getResourceId() {
        return resourceId;
    }

    @Override
    public BranchType getBranchType() {
        return BranchType.XA;
    }

    void beginPhaseOne(String txnId) {
        phaseOne.put(txnId, Boolean.FALSE);
    }

    /**
     * Called by the RM before forwarding a TC rollback to the shim. If the branch is still in phase
     * 1, the shim may not have registered the txn yet (execute not started), so its abort would be a
     * no-op and a later execute/prepare would leave a prepared txn holding locks forever. The flag
     * makes the executor abort the txn itself once execute/prepare return.
     */
    void markRollbackIfInPhaseOne(String txnId) {
        phaseOne.computeIfPresent(txnId, (id, requested) -> Boolean.TRUE);
    }

    boolean isRollbackRequested(String txnId) {
        return Boolean.TRUE.equals(phaseOne.get(txnId));
    }

    void endPhaseOne(String txnId) {
        phaseOne.remove(txnId);
    }

    @Override
    public String toString() {
        return "ShimResource{" + resourceId + "}";
    }
}
