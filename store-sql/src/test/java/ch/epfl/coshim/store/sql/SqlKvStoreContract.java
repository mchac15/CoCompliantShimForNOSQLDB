package ch.epfl.coshim.store.sql;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ch.epfl.coshim.core.Outcome;
import ch.epfl.coshim.core.SpeculativeCoShim;
import ch.epfl.coshim.core.Vote;
import ch.epfl.coshim.store.TableKey;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

/**
 * {@link SqlKvStore} against a real database, run once per dialect by the subclasses when that
 * database's URL is set (e.g. from {@code scripts/bench-dbs.sh start mysql 1}). Tables get a random
 * prefix and are dropped afterwards, so a shared database is left as it was.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
abstract class SqlKvStoreContract {

    private final String prefix = "t" + UUID.randomUUID().toString().replace("-", "").substring(0, 12) + "_";
    private final List<String> created = new ArrayList<>();
    private SqlKvStore store;

    abstract String url();

    @BeforeAll
    void open() {
        store = SqlKvStore.open(url(), 8);
    }

    @AfterAll
    void dropTablesAndClose() throws SQLException {
        try (SqlKvStore owner = store;
                Connection c = owner.dataSourceForTests().getConnection();
                Statement s = c.createStatement()) {
            for (String table : created) {
                s.execute("drop table if exists " + owner.dialect().quote(table));
            }
        }
    }

    private String table(String name) {
        String table = prefix + name;
        created.add(table);
        return table;
    }

    @Test
    void getOfAnAbsentKeyIsNullAndStoreOverwrites() {
        TableKey<String> x = new TableKey<>(table("basic"), "x");
        assertNull(store.get(x));
        store.store(x, "1");
        assertEquals("1", store.get(x));
        store.store(x, "2");
        assertEquals("2", store.get(x));
    }

    @Test
    void tablesAndKeyCaseMakeDifferentKeys() {
        String a = table("a");
        String b = table("b");
        store.store(new TableKey<>(a, "k"), "in a");
        store.store(new TableKey<>(b, "k"), "in b");
        store.store(new TableKey<>(a, "K"), "upper");
        assertEquals("in a", store.get(new TableKey<>(a, "k")));
        assertEquals("in b", store.get(new TableKey<>(b, "k")));
        assertEquals("upper", store.get(new TableKey<>(a, "K")));
    }

    @Test
    void plainKeysGoToTheDefaultTable() {
        // kv may hold other users' rows: only this test's key is touched, the table is kept
        String key = prefix + "plain";
        store.store(TableKey.of(key), "v");
        assertEquals("v", store.get(TableKey.of(key)));
        assertNull(store.get(new TableKey<>(table("other"), key)));
    }

    @Test
    void valuesRoundTripUnchanged() {
        TableKey<String> k = new TableKey<>(table("values"), "ключ 🔑");
        String value = "line 1\nline 2 'quoted' \"double\" é ü 漢字 🚀 " + "x".repeat(100_000);
        store.store(k, value);
        assertEquals(value, store.get(k));
    }

    @Test
    void storeAllWritesEveryEntryAcrossTables() {
        String a = table("batch_a");
        String b = table("batch_b");
        store.store(new TableKey<>(a, "k0"), "old");
        Map<TableKey<String>, String> entries = new HashMap<>();
        for (int i = 0; i < 500; i++) {
            entries.put(new TableKey<>(i % 2 == 0 ? a : b, "k" + i), "v" + i);
        }
        store.storeAll(entries);
        entries.forEach((key, value) -> assertEquals(value, store.get(key)));
    }

    @Test
    void concurrentOverlappingBatchesAllSucceed() throws Exception {
        String t = table("concurrent");
        ExecutorService pool = Executors.newFixedThreadPool(8);
        try {
            List<Future<?>> done = new ArrayList<>();
            for (int w = 0; w < 8; w++) {
                int worker = w;
                done.add(pool.submit(() -> {
                    for (int round = 0; round < 20; round++) {
                        Map<TableKey<String>, String> entries = new HashMap<>();
                        for (int i = 0; i < 20; i++) {
                            entries.put(new TableKey<>(t, "k" + i), worker + ":" + round);
                        }
                        store.storeAll(entries);
                    }
                }));
            }
            for (Future<?> f : done) {
                f.get();
            }
        } finally {
            pool.shutdownNow();
        }
        for (int i = 0; i < 20; i++) {
            assertTrue(store.get(new TableKey<>(t, "k" + i)).endsWith(":19"));
        }
    }

    @Test
    void storeRejectsNullValues() {
        assertThrows(NullPointerException.class, () -> store.store(new TableKey<>(table("nulls"), "k"), null));
    }

    @Test
    void speculativeShimCommitsItsWriteBufferToTheDatabase() {
        TableKey<String> alice = new TableKey<>(table("accounts"), "alice");
        TableKey<String> bob = new TableKey<>(alice.table(), "bob");
        store.storeAll(Map.of(alice, "100", bob, "0"));
        SpeculativeCoShim<TableKey<String>, String> shim = new SpeculativeCoShim<>(store, Duration.ofSeconds(1));

        // t1 moves 30 from alice to bob; t2 reads alice speculatively behind t1 and commits after it
        assertEquals(Outcome.SUCCEEDED, shim.start("t1"));
        shim.put("t1", alice, String.valueOf(Integer.parseInt(shim.get("t1", alice)) - 30));
        shim.put("t1", bob, String.valueOf(Integer.parseInt(shim.get("t1", bob)) + 30));
        assertEquals(Outcome.SUCCEEDED, shim.end("t1"));
        assertEquals("100", store.get(alice), "nothing reaches the database before commit");

        assertEquals(Outcome.SUCCEEDED, shim.start("t2"));
        assertEquals("70", shim.get("t2", alice), "speculative read of t1's buffered write");
        shim.put("t2", alice, "69");
        assertEquals(Outcome.SUCCEEDED, shim.end("t2"));

        assertEquals(Vote.YES, shim.prepare("t1"));
        shim.commit("t1");
        assertEquals("70", store.get(alice));
        assertEquals("30", store.get(bob));
        assertEquals(Vote.YES, shim.prepare("t2"));
        shim.commit("t2");
        assertEquals("69", store.get(alice));

        assertEquals(Outcome.SUCCEEDED, shim.start("t3"));
        shim.put("t3", bob, "999");
        assertEquals(Outcome.SUCCEEDED, shim.end("t3"));
        shim.abort("t3");
        assertEquals("30", store.get(bob), "an aborted txn never touches the database");
    }
}
