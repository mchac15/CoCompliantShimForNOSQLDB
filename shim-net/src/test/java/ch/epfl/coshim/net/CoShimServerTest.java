package ch.epfl.coshim.net;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ch.epfl.coshim.core.NoCcShim;
import ch.epfl.coshim.core.Outcome;
import ch.epfl.coshim.core.TxnAbortedException;
import ch.epfl.coshim.core.Vote;
import ch.epfl.coshim.store.InMemoryKvStore;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class CoShimServerTest {

    private static final String TOKEN = "s3cret";

    private final InMemoryKvStore<String, String> store = new InMemoryKvStore<>();
    private CoShimServer<String, String> server;
    private RemoteCoShim<String, String> client;

    private void startNode(NoCcShim<String, String> shim, Set<InetAddress> allowed) throws Exception {
        startNode(Map.of("shop", shim), allowed);
    }

    private void startNode(Map<String, NoCcShim<String, String>> databases, Set<InetAddress> allowed)
            throws Exception {
        server = new CoShimServer<>(databases, Codec.UTF8, Codec.UTF8,
                new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), TOKEN, allowed);
    }

    private RemoteCoShim<String, String> connect(String token) {
        return connect("shop", token);
    }

    private RemoteCoShim<String, String> connect(String database, String token) {
        return new RemoteCoShim<>(
                new InetSocketAddress(InetAddress.getLoopbackAddress(), server.getPort()),
                database, token, Codec.UTF8, Codec.UTF8);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (client != null) {
            client.close();
        }
        if (server != null) {
            server.close();
        }
    }

    @Test
    void requestsRoundTripToTheNode() throws Exception {
        startNode(new NoCcShim<>(store), Set.of());
        client = connect(TOKEN).verify();

        assertTrue(client.start("t1"));
        assertFalse(client.start("t1"));
        client.put("t1", "x", "1");
        assertEquals("1", client.get("t1", "x"));
        assertNull(client.get("t1", "absent"));
        assertEquals(Outcome.SUCCEEDED, client.end("t1"));
        assertEquals(Vote.YES, client.prepare("t1"));
        assertNull(store.get("x"));
        client.commit("t1");
        assertEquals("1", store.get("x"));
    }

    @Test
    void shimAbortsReachTheClientAsTxnAbortedException() throws Exception {
        startNode(new NoCcShim<>(store), Set.of());
        client = connect(TOKEN);
        assertThrows(TxnAbortedException.class, () -> client.get("unknown", "x"));
        assertEquals(Vote.NO, client.prepare("unknown"), "the connection is still usable afterwards");
    }

    @Test
    void abortBeforeStartIsRememberedByTheNode() throws Exception {
        startNode(new NoCcShim<>(store), Set.of());
        client = connect(TOKEN);
        client.abort("t1");   // the TC's rollback, possibly relayed by another RM instance
        assertFalse(client.start("t1"));
        assertEquals(Vote.NO, client.prepare("t1"));
    }

    @Test
    void databasesOnOneNodeHaveSeparateShimsAndStores() throws Exception {
        InMemoryKvStore<String, String> ordersStore = new InMemoryKvStore<>();
        InMemoryKvStore<String, String> stockStore = new InMemoryKvStore<>();
        startNode(Map.of("orders", new NoCcShim<>(ordersStore), "stock", new NoCcShim<>(stockStore)), Set.of());
        client = connect("orders", TOKEN);
        try (RemoteCoShim<String, String> stock = connect("stock", TOKEN)) {
            // the same txn id and key on two databases are unrelated
            assertTrue(client.start("t1"));
            assertTrue(stock.start("t1"));
            client.put("t1", "k", "order");
            stock.put("t1", "k", "level");
            for (RemoteCoShim<String, String> db : java.util.List.of(client, stock)) {
                db.end("t1");
                db.prepare("t1");
                db.commit("t1");
            }
        }
        assertEquals("order", ordersStore.get("k"));
        assertEquals("level", stockStore.get("k"));
    }

    @Test
    void unknownDatabaseIsRejected() throws Exception {
        startNode(new NoCcShim<>(store), Set.of());
        assertThrows(SecurityException.class, () -> connect("nope", TOKEN).verify());
    }

    @Test
    void wrongTokenIsRejected() throws Exception {
        startNode(new NoCcShim<>(store), Set.of());
        assertThrows(SecurityException.class, () -> connect("guess").verify());
    }

    @Test
    void clientsOutsideTheAllowlistAreRejected() throws Exception {
        startNode(new NoCcShim<>(store), Set.of(InetAddress.getByName("192.0.2.1")));   // not loopback
        assertThrows(SecurityException.class, () -> connect(TOKEN).verify());
    }

    @Test
    void aBlockedRequestDoesNotHoldUpOtherTransactions() throws Exception {
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch blocked = new CountDownLatch(1);
        startNode(new NoCcShim<>(store) {
            @Override
            public Vote prepare(String txnId) {
                if (txnId.equals("slow")) {   // stands in for prepare waiting on predecessors
                    blocked.countDown();
                    try {
                        release.await(10, TimeUnit.SECONDS);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
                return super.prepare(txnId);
            }
        }, Set.of());
        client = connect(TOKEN);

        client.start("slow");
        client.end("slow");
        CompletableFuture<Vote> slowVote = CompletableFuture.supplyAsync(() -> client.prepare("slow"));
        assertTrue(blocked.await(10, TimeUnit.SECONDS));

        // another transaction runs to completion meanwhile, on another pooled connection
        client.start("fast");
        client.put("fast", "y", "2");
        client.end("fast");
        assertEquals(Vote.YES, client.prepare("fast"));
        client.commit("fast");
        assertEquals("2", store.get("y"));
        assertFalse(slowVote.isDone());

        release.countDown();
        assertEquals(Vote.YES, slowVote.get(10, TimeUnit.SECONDS));
    }
}
