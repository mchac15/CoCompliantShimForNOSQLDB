package ch.epfl.coshim.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ch.epfl.coshim.core.NoCcShim;
import ch.epfl.coshim.core.Outcome;
import ch.epfl.coshim.store.InMemoryKvStore;
import ch.epfl.coshim.store.TableKey;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * The connection serializes its get/put and the end of its transaction, so the shim never sees them
 * overlap for one transaction, even when the application shares the connection between threads; aborts
 * are not serialized; a request with an unknown outcome makes the transaction fail.
 */
class CoShimConnectionSerializationTest {

    private static final String T = "orders";
    private static final TableKey<String> SLOW = new TableKey<>(T, "slow");

    private final Xid xid = new CoShimXAResourceTest.TestXid("127.0.0.1:8091:42", "-7");

    /** A put of SLOW blocks until {@link #release} opens; records the order in which requests run. */
    private static final class BlockingShim extends NoCcShim<TableKey<String>, String> {
        final CountDownLatch slowPutEntered = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        final List<String> events = Collections.synchronizedList(new ArrayList<>());
        final AtomicInteger inFlight = new AtomicInteger();
        final AtomicInteger maxInFlight = new AtomicInteger();
        volatile RuntimeException failSlowPut;

        BlockingShim(InMemoryKvStore<TableKey<String>, String> store) {
            super(store);
        }

        private void enter() {
            maxInFlight.accumulateAndGet(inFlight.incrementAndGet(), Math::max);
        }

        @Override
        public String get(String txnId, TableKey<String> key) {
            enter();
            try {
                sleepBriefly();   // widens the window in which an unserialized second request would overlap
                return super.get(txnId, key);
            } finally {
                inFlight.decrementAndGet();
            }
        }

        @Override
        public void put(String txnId, TableKey<String> key, String value) {
            enter();
            try {
                if (key.equals(SLOW)) {
                    slowPutEntered.countDown();
                    await(release);
                    if (failSlowPut != null) {
                        throw failSlowPut;
                    }
                }
                super.put(txnId, key, value);
                events.add("put " + key.key());
            } finally {
                inFlight.decrementAndGet();
            }
        }

        @Override
        public Outcome end(String txnId) {
            events.add("end");
            return super.end(txnId);
        }

        @Override
        public void abort(String txnId) {
            events.add("abort");
            super.abort(txnId);
        }
    }

    private InMemoryKvStore<TableKey<String>, String> store;
    private BlockingShim shim;
    private CoShimDataSource<String, String> dataSource;
    private ExecutorService threads;

    @BeforeEach
    void setUp() {
        store = new InMemoryKvStore<>();
        shim = new BlockingShim(store);
        dataSource = new CoShimDataSource<>("localhost:7000/shop", shim);
        threads = Executors.newCachedThreadPool();
    }

    @AfterEach
    void tearDown() {
        shim.release.countDown();
        threads.shutdownNow();
    }

    @Test
    void xaEndWaitsForAPutInFlightOnAnotherThread() throws Exception {
        CoShimXAConnection xa = dataSource.getXAConnection();
        XAResource res = xa.getXAResource();
        KvSession<String, String> kv = KvSession.from(xa.getConnection());
        res.start(xid, XAResource.TMNOFLAGS);

        Future<?> put = threads.submit(() -> {
            kv.put(T, "slow", "1");
            return null;
        });
        assertTrue(shim.slowPutEntered.await(5, TimeUnit.SECONDS));
        Future<?> end = threads.submit(() -> {
            res.end(xid, XAResource.TMSUCCESS);
            return null;
        });
        Thread.sleep(100);
        assertFalse(end.isDone(), "xa end must wait for the put");
        assertFalse(shim.events.contains("end"), "the shim's end must not run while the put is in flight");

        shim.release.countDown();
        put.get(5, TimeUnit.SECONDS);
        end.get(5, TimeUnit.SECONDS);
        assertEquals(List.of("put slow", "end"), shim.events);

        res.prepare(xid);
        res.commit(xid, false);
        assertEquals("1", store.get(SLOW), "the write made it into the committed transaction");
    }

    @Test
    void getsAndPutsOfASharedConnectionNeverOverlapOnTheShim() throws Exception {
        CoShimXAConnection xa = dataSource.getXAConnection();
        KvSession<String, String> kv = KvSession.from(xa.getConnection());
        xa.getXAResource().start(xid, XAResource.TMNOFLAGS);

        List<Future<?>> requests = new ArrayList<>();
        for (int i = 0; i < 8; i++) {
            String key = "k" + i;
            requests.add(threads.submit(() -> {
                kv.put(T, key, "v");
                return kv.get(T, key);
            }));
        }
        for (Future<?> r : requests) {
            assertEquals("v", r.get(5, TimeUnit.SECONDS), "concurrent requests wait, none fails");
        }
        assertEquals(1, shim.maxInFlight.get(), "at most one request of the transaction on the shim at a time");
    }

    @Test
    void coordinatorRollbackIsNotBlockedByAPutInFlight() throws Exception {
        CoShimXAConnection xa = dataSource.getXAConnection();
        KvSession<String, String> kv = KvSession.from(xa.getConnection());
        xa.getXAResource().start(xid, XAResource.TMNOFLAGS);

        Future<?> put = threads.submit(() -> {
            kv.put(T, "slow", "1");
            return null;
        });
        assertTrue(shim.slowPutEntered.await(5, TimeUnit.SECONDS));

        // what Seata's ConnectionProxyXA / ResourceManagerXA do on a rollback, while the put is blocked
        Future<?> rollback = threads.submit(() -> {
            xa.getXAResource().end(xid, XAResource.TMFAIL);
            dataSource.getXAConnection().getXAResource().rollback(xid);
            return null;
        });
        rollback.get(5, TimeUnit.SECONDS);
        assertTrue(shim.events.contains("abort"));
        assertFalse(put.isDone(), "the rollback went through while the put was still blocked");
        shim.release.countDown();
    }

    @Test
    void aRequestWithAnUnknownOutcomeFailsTheBranchWithoutReachingTheShimsEnd() throws Exception {
        CoShimXAConnection xa = dataSource.getXAConnection();
        XAResource res = xa.getXAResource();
        KvSession<String, String> kv = KvSession.from(xa.getConnection());
        res.start(xid, XAResource.TMNOFLAGS);

        shim.failSlowPut = new UncheckedIOException("shim node unreachable", new IOException("reset"));
        shim.release.countDown();
        assertThrows(UncheckedIOException.class, () -> kv.put(T, "slow", "1"));

        XAException e = assertThrows(XAException.class, () -> res.end(xid, XAResource.TMSUCCESS));
        assertEquals(XAException.XA_RBROLLBACK, e.errorCode);
        assertFalse(shim.events.contains("end"), "the put may still be running on the shim: never end it");
    }

    @Test
    void aLocalRequestWithAnUnknownOutcomeAbortsTheLocalTransaction() throws Exception {
        CoShimConnection<String, String> c = dataSource.getConnection();
        c.setAutoCommit(false);
        c.put(T, "x", "1");

        shim.failSlowPut = new UncheckedIOException("shim node unreachable", new IOException("reset"));
        shim.release.countDown();
        assertThrows(UncheckedIOException.class, () -> c.put(T, "slow", "1"));
        assertTrue(shim.events.contains("abort"), "this connection coordinates it, so it aborts it at once");

        c.commit();   // nothing left to commit
        assertFalse(shim.events.contains("end"));
        assertNull(store.get(new TableKey<>(T, "x")));
    }

    private static void sleepBriefly() {
        try {
            Thread.sleep(5);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static void await(CountDownLatch latch) {
        try {
            latch.await(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
