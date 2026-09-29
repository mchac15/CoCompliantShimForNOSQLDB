package ch.epfl.coshim.seata;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import ch.epfl.coshim.core.NoCcShim;
import ch.epfl.coshim.jdbc.CoShimDataSource;
import ch.epfl.coshim.jdbc.KvSession;
import ch.epfl.coshim.net.Codec;
import ch.epfl.coshim.net.CoShimServer;
import ch.epfl.coshim.net.RemoteCoShim;
import ch.epfl.coshim.store.InMemoryKvStore;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.sql.Connection;
import java.util.Set;
import org.apache.seata.core.context.RootContext;
import org.apache.seata.core.model.BranchStatus;
import org.apache.seata.core.model.BranchType;
import org.apache.seata.rm.DefaultResourceManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * The shim as a separate node: Seata's XA classes in the "application" talk to a CoShimServer over
 * TCP through RemoteCoShim. The resource id is the node's address, like a MySQL URL, so any RM
 * instance can finish a branch in phase 2.
 */
class RemoteShimNodeTest {

    private static final String XID = "127.0.0.1:8091:77";
    private static final String TOKEN = "s3cret";

    private InMemoryKvStore<String, String> nodeStore;
    private CoShimServer<String, String> node;
    private RemoteCoShim<String, String> appA;
    private RemoteCoShim<String, String> appB;
    private TestResourceManagerXA rm;
    private String name;

    @BeforeEach
    void setUp() throws Exception {
        nodeStore = new InMemoryKvStore<>();
        node = new CoShimServer<>(new NoCcShim<>(nodeStore), Codec.UTF8, Codec.UTF8,
                new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), TOKEN, Set.of());
        InetSocketAddress address = new InetSocketAddress(InetAddress.getLoopbackAddress(), node.getPort());
        appA = new RemoteCoShim<>(address, TOKEN, Codec.UTF8, Codec.UTF8);
        appB = new RemoteCoShim<>(address, TOKEN, Codec.UTF8, Codec.UTF8);
        name = "127.0.0.1:" + node.getPort() + "/orders-kv";

        rm = new TestResourceManagerXA();
        DefaultResourceManager.get();
        DefaultResourceManager.mockResourceManager(BranchType.XA, rm);
    }

    @AfterEach
    void tearDown() throws Exception {
        RootContext.unbind();
        appA.close();
        appB.close();
        node.close();
    }

    @Test
    void phaseOneOnOneRmAndPhaseTwoOnAnotherBothReachTheNode() throws Exception {
        DataSourceProxyCoShim instanceA = new DataSourceProxyCoShim(new CoShimDataSource<>(name, appA));
        assertEquals("jdbc:coshim://" + name, instanceA.getResourceId(), "the node's address is the resource id");

        RootContext.bind(XID);
        try (Connection c = instanceA.getConnection()) {
            c.setAutoCommit(false);
            KvSession<String, String> kv = KvSession.from(c);
            kv.put("x", "1");
            assertEquals("1", kv.get("x"));
            c.commit();
        }
        RootContext.unbind();
        assertNull(nodeStore.get("x"));

        // a second application instance registers the same resource id; the TC may route phase 2 to it
        new DataSourceProxyCoShim(new CoShimDataSource<>(name, appB));
        TestResourceManagerXA.Registration reg = rm.registrations.get(0);
        assertEquals(
                BranchStatus.PhaseTwo_Committed,
                rm.branchCommit(BranchType.XA, XID, reg.branchId(), reg.resourceId(), null));
        assertEquals("1", nodeStore.get("x"));
    }
}
