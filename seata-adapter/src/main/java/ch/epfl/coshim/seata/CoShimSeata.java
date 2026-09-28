package ch.epfl.coshim.seata;

import org.apache.seata.core.model.BranchType;
import org.apache.seata.core.model.ResourceManager;
import org.apache.seata.rm.DefaultResourceManager;

/** Entry point for applications: registers shims with Seata. */
public final class CoShimSeata {

    private CoShimSeata() {}

    /**
     * Registers {@code resource} with the XA resource manager (and so with the TC). Call it once per
     * shim after {@code RMClient.init(...)}.
     */
    public static void register(ShimResource<?, ?> resource) {
        routingResourceManager().registerResource(resource);
    }

    /**
     * The XA resource manager, which the SPI file of this module makes a
     * {@link RoutingResourceManagerXA}. Fails loudly if another jar overrode it, instead of silently
     * sending shim phase-2 requests to the stock XA RM (which would answer "unknown resource").
     */
    public static RoutingResourceManagerXA routingResourceManager() {
        ResourceManager rm = DefaultResourceManager.get().getResourceManager(BranchType.XA);
        if (!(rm instanceof RoutingResourceManagerXA)) {
            throw new IllegalStateException("XA resource manager is " + rm.getClass().getName()
                    + ", expected " + RoutingResourceManagerXA.class.getName() + " (check META-INF/services)");
        }
        return (RoutingResourceManagerXA) rm;
    }
}
