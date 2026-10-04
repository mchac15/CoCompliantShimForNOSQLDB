package ch.epfl.coshim.seata;

import static org.junit.jupiter.api.Assertions.assertEquals;

import ch.epfl.coshim.core.SpeculativeCoShim;
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
import java.time.Duration;
import java.util.List;
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
 * End-to-end smoke test: two shim nodes, each with its own local store and a {@link
 * SpeculativeCoShim}, and global transactions spanning both through Seata's stock XA classes
 * (ConnectionProxyXA for phase 1, ResourceManagerXA for phase 2). The test plays the TC: it runs
 * phase 1 on both branches, then decides commit or rollback for the whole global transaction.
 *
 * <p>Each global transaction moves money from alice (node A) to bob (node B). Consistency check:
 * only the committed transfers are visible, and the total stays the same.
 */
class TwoNodeSeataIntegrationTest {

    private static final String TOKEN = "s3cret";
    private static final String T = "accounts";
    private static final int INITIAL = 100;

    private Node nodeA;
    private Node nodeB;
    private TestResourceManagerXA rm;
    private int nextXid = 1;

    /** A shim node hosting one database, and the application-side data source that reaches it. */
    private static final class Node implements AutoCloseable {
        final InMemoryKvStore<TableKey<String>, String> store = new InMemoryKvStore<>();
        final CoShimServer<TableKey<String>, String> server;
        final RemoteCoShim<TableKey<String>, String> client;
        DataSourceProxyCoShim dataSource;

        Node(String db) throws Exception {
            server = new CoShimServer<>(
                    Map.of(db, new SpeculativeCoShim<>(store, Duration.ofSeconds(5))),
                    new TableKeyCodec<>(Codec.UTF8), Codec.UTF8,
                    new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), TOKEN, Set.of());
            client = new RemoteCoShim<>(new InetSocketAddress(InetAddress.getLoopbackAddress(), server.getPort()),
                    db, TOKEN, new TableKeyCodec<>(Codec.UTF8), Codec.UTF8);
        }

        void connect(String db) {
            dataSource = new DataSourceProxyCoShim(
                    new CoShimDataSource<>("127.0.0.1:" + server.getPort() + "/" + db, client));
        }

        int balance(String account) {
            return Integer.parseInt(store.get(new TableKey<>(T, account)));
        }

        @Override
        public void close() throws Exception {
            client.close();
            server.close();
        }
    }

    @BeforeEach
    void setUp() throws Exception {
        rm = new TestResourceManagerXA();
        DefaultResourceManager.get();   // its lazy SPI init would otherwise overwrite the mock
        DefaultResourceManager.mockResourceManager(BranchType.XA, rm);

        nodeA = new Node("bank-a");
        nodeB = new Node("bank-b");
        nodeA.connect("bank-a");
        nodeB.connect("bank-b");
        // initial state, written before any transaction runs
        nodeA.store.store(new TableKey<>(T, "alice"), String.valueOf(INITIAL));
        nodeB.store.store(new TableKey<>(T, "bob"), String.valueOf(INITIAL));
    }

    @AfterEach
    void tearDown() throws Exception {
        RootContext.unbind();
        nodeA.close();
        nodeB.close();
    }

    /** Phase 1 of one branch: read the balance, add delta, enlisted in the bound global transaction. */
    private TestResourceManagerXA.Registration addTo(Node node, String account, int delta) throws SQLException {
        try (Connection c = node.dataSource.getConnection()) {
            c.setAutoCommit(false);   // branch register + xa start
            KvSession<String, String> kv = KvSession.from(c);
            int balance = Integer.parseInt(kv.get(T, account));
            kv.put(T, account, String.valueOf(balance + delta));
            c.commit();   // xa end + xa prepare: the shim votes
        }
        return rm.registrations.get(rm.registrations.size() - 1);
    }

    /** Phase 1 of a transfer on both nodes, under one global transaction id. */
    private List<TestResourceManagerXA.Registration> transfer(int amount) throws SQLException {
        RootContext.bind("127.0.0.1:8091:" + nextXid++);
        try {
            return List.of(addTo(nodeA, "alice", -amount), addTo(nodeB, "bob", amount));
        } finally {
            RootContext.unbind();
        }
    }

    /** Phase 2 as the TC sends it after a global commit. */
    private void globalCommit(List<TestResourceManagerXA.Registration> branches) throws Exception {
        for (TestResourceManagerXA.Registration b : branches) {
            assertEquals(BranchStatus.PhaseTwo_Committed,
                    rm.branchCommit(BranchType.XA, b.xid(), b.branchId(), b.resourceId(), null));
        }
    }

    /** Phase 2 as the TC sends it after a global rollback. */
    private void globalRollback(List<TestResourceManagerXA.Registration> branches) throws Exception {
        for (TestResourceManagerXA.Registration b : branches) {
            assertEquals(BranchStatus.PhaseTwo_Rollbacked,
                    rm.branchRollback(BranchType.XA, b.xid(), b.branchId(), b.resourceId(), null));
        }
    }

    private void assertBalances(int alice, int bob) {
        assertEquals(alice, nodeA.balance("alice"));
        assertEquals(bob, nodeB.balance("bob"));
        assertEquals(2 * INITIAL, nodeA.balance("alice") + nodeB.balance("bob"), "money is conserved");
    }

    @Test
    void committedTransfersAreAppliedOnBothNodesAndRolledBackOnesOnNeither() throws Exception {
        globalCommit(transfer(30));
        assertBalances(70, 130);

        List<TestResourceManagerXA.Registration> aborted = transfer(50);
        assertBalances(70, 130);   // prepared on both nodes, nothing applied before phase 2
        globalRollback(aborted);
        assertBalances(70, 130);

        // the rolled-back transfer released its locks: the next one sees the committed state
        globalCommit(transfer(20));
        assertBalances(50, 150);

        globalRollback(transfer(10));
        assertBalances(50, 150);
    }

    @Test
    void branchFailingPhaseOneRollsBackTheWholeGlobalTransaction() throws Exception {
        RootContext.bind("127.0.0.1:8091:" + nextXid++);
        TestResourceManagerXA.Registration onA;
        try {
            onA = addTo(nodeA, "alice", -40);   // prepared on node A
            try (Connection c = nodeB.dataSource.getConnection()) {
                c.setAutoCommit(false);
                KvSession.<String, String>from(c).put(T, "bob", "140");
                c.rollback();   // the application gives up on node B: phase 1 fails there
            }
        } finally {
            RootContext.unbind();
        }
        // the TC rolls the global transaction back, including the branch that was already prepared
        globalRollback(List.of(onA));
        assertBalances(INITIAL, INITIAL);

        // both nodes are usable again
        globalCommit(transfer(5));
        assertBalances(95, 105);
    }
}
