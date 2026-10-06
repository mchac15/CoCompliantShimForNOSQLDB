package ch.epfl.coshim.store.sql;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ch.epfl.coshim.store.TableKey;
import ch.epfl.coshim.store.sql.SqlKvStore.Dialect;
import java.lang.reflect.Proxy;
import java.sql.BatchUpdateException;
import java.sql.SQLException;
import java.sql.SQLTransientConnectionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntFunction;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;

/** What needs no database; the SQL itself runs in {@link SqlKvStoreContract}. */
class SqlKvStoreTest {

    /** A data source whose n-th getConnection() (from 1) throws {@code failure.apply(n)}. */
    private static DataSource failing(AtomicInteger calls, IntFunction<SQLException> failure) {
        return (DataSource) Proxy.newProxyInstance(DataSource.class.getClassLoader(),
                new Class<?>[] {DataSource.class}, (proxy, method, args) -> {
                    throw failure.apply(calls.incrementAndGet());
                });
    }

    @Test
    void dialectComesFromTheJdbcUrl() {
        assertEquals(Dialect.MYSQL, Dialect.of("jdbc:mysql://127.0.0.1:3306/acta"));
        assertEquals(Dialect.POSTGRESQL, Dialect.of("jdbc:postgresql://127.0.0.1:5432/acta"));
        assertThrows(IllegalArgumentException.class, () -> Dialect.of("jdbc:h2:mem:x"));
    }

    @Test
    void statementsQuoteTheTableForTheDialect() {
        assertEquals("select v from `orders` where k = ?", Dialect.MYSQL.select("orders"));
        assertEquals("select v from \"orders\" where k = ?", Dialect.POSTGRESQL.select("orders"));
        assertTrue(Dialect.MYSQL.upsert("orders").endsWith("on duplicate key update v = values(v)"));
        assertTrue(Dialect.POSTGRESQL.upsert("orders").endsWith("on conflict (k) do update set v = excluded.v"));
    }

    @Test
    void tableNamesThatAreNotPlainIdentifiersAreRejectedBeforeAnySql() {
        AtomicInteger calls = new AtomicInteger();
        SqlKvStore store = new SqlKvStore(failing(calls, n -> new SQLException("no database")), Dialect.POSTGRESQL);
        for (String table : new String[] {"a\"; drop table kv; --", "x y", "1abc", "", "a".repeat(64)}) {
            assertThrows(IllegalArgumentException.class, () -> store.get(new TableKey<>(table, "k")), table);
        }
        assertEquals(0, calls.get());
    }

    @Test
    void nonTransientErrorsAreThrownNotRetried() {
        AtomicInteger calls = new AtomicInteger();
        SqlKvStore store = new SqlKvStore(
                failing(calls, n -> new SQLException("password authentication failed", "28P01")), Dialect.POSTGRESQL);
        assertThrows(SqlKvStore.SqlStoreException.class, () -> store.get(TableKey.of("k")));
        assertEquals(1, calls.get());
    }

    @Test
    void transientErrorsAreRetried() {
        AtomicInteger calls = new AtomicInteger();
        SqlKvStore store = new SqlKvStore(failing(calls, n -> n < 3
                ? new SQLTransientConnectionException("pool exhausted")
                : new SQLException("permission denied", "42501")), Dialect.POSTGRESQL);
        assertThrows(SqlKvStore.SqlStoreException.class, () -> store.get(TableKey.of("k")));
        assertEquals(3, calls.get(), "two transient failures retried, then the real error thrown");
    }

    @Test
    void transientIsJudgedOnSqlStateAndChainedExceptions() {
        assertTrue(SqlKvStore.isTransient(new SQLException("deadlock detected", "40P01")));
        assertTrue(SqlKvStore.isTransient(new SQLException("could not serialize", "40001")));
        assertTrue(SqlKvStore.isTransient(new SQLException("connection refused", "08001")));
        BatchUpdateException batch = new BatchUpdateException("batch failed", "HY000", new int[0]);
        batch.setNextException(new SQLException("deadlock detected", "40P01"));
        assertTrue(SqlKvStore.isTransient(batch));
        assertFalse(SqlKvStore.isTransient(new SQLException("syntax error", "42601")));
        assertFalse(SqlKvStore.isTransient(new SQLException("no state")));
    }
}
