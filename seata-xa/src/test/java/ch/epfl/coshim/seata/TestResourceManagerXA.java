package ch.epfl.coshim.seata;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.seata.core.model.BranchStatus;
import org.apache.seata.core.model.BranchType;
import org.apache.seata.core.model.Resource;
import org.apache.seata.rm.datasource.xa.ResourceManagerXA;

/**
 * Stock ResourceManagerXA without a TC: resource registration, branch register and branch report
 * are recorded locally instead of going over Netty (the pattern of Seata's own XAModeTest2).
 * Phase 2 (branchCommit/branchRollback) is the unmodified stock code.
 */
class TestResourceManagerXA extends ResourceManagerXA {

    record Registration(long branchId, String resourceId, String xid) {}

    record Report(long branchId, BranchStatus status) {}

    interface AfterRegister {
        void run(Registration registration) throws Exception;
    }

    final List<Registration> registrations = new CopyOnWriteArrayList<>();
    final List<Report> reports = new CopyOnWriteArrayList<>();
    private final AtomicLong nextBranchId = new AtomicLong(1);

    /** Runs inside branchRegister, before it returns: simulates TC messages racing phase 1. */
    volatile AfterRegister afterRegister = registration -> {};

    @Override
    public void registerResource(Resource resource) {
        dataSourceCache.put(resource.getResourceId(), resource);
    }

    @Override
    public Long branchRegister(
            BranchType branchType,
            String resourceId,
            String clientId,
            String xid,
            String applicationData,
            String lockKeys) {
        Registration registration = new Registration(nextBranchId.getAndIncrement(), resourceId, xid);
        registrations.add(registration);
        try {
            afterRegister.run(registration);
        } catch (Exception e) {
            throw new AssertionError(e);
        }
        return registration.branchId();
    }

    @Override
    public void branchReport(
            BranchType branchType, String xid, long branchId, BranchStatus status, String applicationData) {
        reports.add(new Report(branchId, status));
    }
}
