package ch.epfl.coshim.seata;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import ch.epfl.coshim.core.NoCcShim;
import ch.epfl.coshim.jdbc.CoShimDataSource;
import ch.epfl.coshim.jdbc.KvSession;
import ch.epfl.coshim.net.Codec;
import ch.epfl.coshim.net.CoShimServer;
import ch.epfl.coshim.net.RemoteCoShim;
import ch.epfl.coshim.net.TableKeyCodec;
import ch.epfl.coshim.store.InMemoryKvStore;
import ch.epfl.coshim.store.TableKey;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;
import java.util.Set;
import org.apache.seata.core.context.RootContext;
import org.apache.seata.core.model.BranchStatus;
import org.apache.seata.core.model.BranchType;
import org.apache.seata.rm.DefaultResourceManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * The shim on a separate node: Seata's XA classes in the "application" talk to a CoShimServer over
 * TCP through RemoteCoShim. A data source is (node, database); its resource id is that address, like
 * a MySQL URL, so any RM instance can finish a branch in phase 2, or relay the TC's rollback.
 */
class RemoteShimNodeTest {

    private static final String XID = "127.0.0.1:8091:77";
    private static final String TOKEN = "s3cret";
    private static final String DB = "shop";

    private InMemoryKvStore<TableKey<String>, String> nodeStore;
    private CoShimServer<TableKey<String>, String> node;
    private RemoteCoShim<TableKey<String>, String> appA;
    private RemoteCoShim<TableKey<String>, String> appB;
    private TestResourceManagerXA rm;
    private String name;

    @BeforeEach
    void setUp() throws Exception {
        nodeStore = new InMemoryKvStore<>();
        node = new CoShimServer<>(Map.of(DB, new NoCcShim<>(nodeStore)), new TableKeyCodec<>(Codec.UTF8), Codec.UTF8,
                new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), TOKEN, Set.of());
        appA = client();
        appB = client();
        name = "127.0.0.1:" + node.getPort() + "/" + DB;

        rm = new TestResourceManagerXA();
        DefaultResourceManager.get();
        DefaultResourceManager.mockResourceManager(BranchType.XA, rm);
    }

    private RemoteCoShim<TableKey<String>, String> client() {
        return new RemoteCoShim<>(new InetSocketAddress(InetAddress.getLoopbackAddress(), node.getPort()),
                DB, TOKEN, new TableKeyCodec<>(Codec.UTF8), Codec.UTF8);
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
        assertEquals("jdbc:coshim://" + name, instanceA.getResourceId(), "node address + database = resource id");

        RootContext.bind(XID);
        try (Connection c = instanceA.getConnection()) {
            c.setAutoCommit(false);
            KvSession<String, String> kv = KvSession.from(c);
            kv.put("orders", "1", "an order");
            kv.put("stock", "1", "a level");   // another table of the same database, same branch
            assertEquals("an order", kv.get("orders", "1"));
            c.commit();
        }
        RootContext.unbind();
        assertNull(nodeStore.get(new TableKey<>("orders", "1")));

        // a second application instance registers the same resource id; the TC may route phase 2 to it
        new DataSourceProxyCoShim(new CoShimDataSource<>(name, appB));
        TestResourceManagerXA.Registration reg = rm.registrations.get(0);
        assertEquals(
                BranchStatus.PhaseTwo_Committed,
                rm.branchCommit(BranchType.XA, XID, reg.branchId(), reg.resourceId(), null));
        assertEquals("an order", nodeStore.get(new TableKey<>("orders", "1")));
        assertEquals("a level", nodeStore.get(new TableKey<>("stock", "1")));
    }

    @Test
    void rollbackRelayedByAnotherInstanceBeforeXaStartStillStopsTheBranch() throws Exception {
        DataSourceProxyCoShim instanceA = new DataSourceProxyCoShim(new CoShimDataSource<>(name, appA));
        // the TC rolls the branch back right after registration and routes it to instance B: the abort
        // reaches the node before instance A's xa start, and the node's tombstone refuses that start
        DataSourceProxyCoShim instanceB = new DataSourceProxyCoShim(new CoShimDataSource<>(name, appB));
        rm.afterRegister = reg -> rm.branchRollback(BranchType.XA, reg.xid(), reg.branchId(), reg.resourceId(), null);

        RootContext.bind(XID);
        try (Connection c = instanceA.getConnection()) {
            assertThrows(SQLException.class, () -> c.setAutoCommit(false), "xa start refused by the node");
            assertThrows(SQLException.class, () -> KvSession.<String, String>from(c).put("orders", "1", "x"));
        }
        assertNull(nodeStore.get(new TableKey<>("orders", "1")));
    }
}
