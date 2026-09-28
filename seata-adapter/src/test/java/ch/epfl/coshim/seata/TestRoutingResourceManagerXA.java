package ch.epfl.coshim.seata;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.seata.core.model.BranchStatus;
import org.apache.seata.core.model.BranchType;
import org.apache.seata.core.model.Resource;

/**
 * RoutingResourceManagerXA without a TC: registration and branch register/report are recorded
 * locally instead of going over Netty (the pattern of Seata's own XAModeTest2).
 */
class TestRoutingResourceManagerXA extends RoutingResourceManagerXA {

    record Registration(long branchId, String resourceId, String xid, String applicationData) {}

    record Report(long branchId, BranchStatus status) {}

    final List<Registration> registrations = new CopyOnWriteArrayList<>();
    final List<Report> reports = new CopyOnWriteArrayList<>();
    private final AtomicLong nextBranchId = new AtomicLong(1);

    @Override
    protected void registerWithTc(Resource resource) {}

    @Override
    public Long branchRegister(
            BranchType branchType,
            String resourceId,
            String clientId,
            String xid,
            String applicationData,
            String lockKeys) {
        long branchId = nextBranchId.getAndIncrement();
        registrations.add(new Registration(branchId, resourceId, xid, applicationData));
        return branchId;
    }

    @Override
    public void branchReport(
            BranchType branchType, String xid, long branchId, BranchStatus status, String applicationData) {
        reports.add(new Report(branchId, status));
    }
}
