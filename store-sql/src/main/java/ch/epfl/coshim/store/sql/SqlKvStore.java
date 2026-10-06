package ch.epfl.coshim.store.sql;

import ch.epfl.coshim.store.KvStore;
import ch.epfl.coshim.store.TableKey;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLRecoverableException;
import java.sql.SQLTransientException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;
import javax.sql.DataSource;

/**
 * A MySQL or PostgreSQL database seen as the shim's {@link KvStore}: {@code get} is a
 * {@code select}, {@code store} an upsert. The database is only storage here. The shim does the
 * concurrency control, so every statement runs on its own in autocommit at READ COMMITTED, and no
 * SQL transaction spans two calls.
 *
 * <p>Each table of a {@link TableKey} is one SQL table {@code (k, v)}: {@code k} is the primary key,
 * {@code v} the value, both strings. Plain keys (no table) go to {@link #DEFAULT_TABLE}. A table is
 * created on first use. Table names must be plain identifiers ({@code [A-Za-z_][A-Za-z0-9_]*}, at
 * most 63 characters), so that they can never inject SQL. Keys compare byte for byte (MySQL:
 * {@code utf8mb4_bin}), as the shim's locks do: {@code "a"} and {@code "A"} are different keys.
 *
 * <p>As {@link KvStore} requires, the operations block and retry until they succeed when the error
 * is transient (lost connection, deadlock, serialization failure, lock wait timeout, pool
 * exhausted). Retrying a blind upsert is idempotent. Any other error (bad credentials, missing
 * privileges, ...) is a configuration problem and is thrown as {@link SqlStoreException}.
 *
 * <p>The connection setup follows the benchmark's MySQL/PG participants (Hikari pool, batched
 * statement rewriting, default users of {@code scripts/bench-dbs.sh}).
 */
public final class SqlKvStore implements KvStore<TableKey<String>, String>, AutoCloseable {

    /** Where plain keys (no table) are stored. */
    public static final String DEFAULT_TABLE = "kv";

    /** Default size of the connection pool of {@link #open(String)}. */
    public static final int DEFAULT_POOL_SIZE = 32;

    private static final Pattern IDENTIFIER = Pattern.compile("[A-Za-z_][A-Za-z0-9_]{0,62}");
    private static final long MAX_BACKOFF_MS = 1_000;

    /** The SQL a backend differs in. */
    public enum Dialect {
        MYSQL('`', "root") {
            @Override
            String createTable(String table) {
                // varchar(255) so that it can be the primary key (utf8mb4: 1020 bytes < 3072);
                // binary collation, otherwise keys differing only in case would be one row
                return "create table if not exists " + quote(table)
                        + " (k varchar(255) not null primary key, v longtext not null)"
                        + " character set utf8mb4 collate utf8mb4_bin";
            }

            @Override
            String upsert(String table) {
                return "insert into " + quote(table) + " (k, v) values (?, ?) on duplicate key update v = values(v)";
            }
        },
        POSTGRESQL('"', "acta") {
            @Override
            String createTable(String table) {
                return "create table if not exists " + quote(table) + " (k text not null primary key, v text not null)";
            }

            @Override
            String upsert(String table) {
                return "insert into " + quote(table) + " (k, v) values (?, ?) on conflict (k) do update set v = excluded.v";
            }
        };

        private final char quote;
        private final String defaultUser;

        Dialect(char quote, String defaultUser) {
            this.quote = quote;
            this.defaultUser = defaultUser;
        }

        /** The dialect of a {@code jdbc:mysql:} or {@code jdbc:postgresql:} URL. */
        public static Dialect of(String jdbcUrl) {
            if (jdbcUrl.startsWith("jdbc:mysql:")) {
                return MYSQL;
            }
            if (jdbcUrl.startsWith("jdbc:postgresql:")) {
                return POSTGRESQL;
            }
            throw new IllegalArgumentException("only jdbc:mysql: and jdbc:postgresql: databases are supported: " + jdbcUrl);
        }

        String quote(String identifier) {
            return quote + identifier + quote;
        }

        String select(String table) {
            return "select v from " + quote(table) + " where k = ?";
        }

        abstract String createTable(String table);

        abstract String upsert(String table);
    }

    /** A non-transient database error: retrying would not help. */
    public static final class SqlStoreException extends RuntimeException {
        SqlStoreException(String message, SQLException cause) {
            super(message, cause);
        }
    }

    @FunctionalInterface
    private interface SqlCall<T> {
        T run(Connection c) throws SQLException;
    }

    private final DataSource dataSource;
    private final Dialect dialect;
    private final AutoCloseable owned;
    /** Tables known to exist (created by this store, or found by {@code create table if not exists}). */
    private final Set<String> tables = ConcurrentHashMap.newKeySet();

    /**
     * Uses connections from {@code dataSource}, which the caller keeps owning: {@link #close} does
     * not close it. The connections must allow autocommit statements.
     */
    public SqlKvStore(DataSource dataSource, Dialect dialect) {
        this(dataSource, dialect, null);
    }

    private SqlKvStore(DataSource dataSource, Dialect dialect, AutoCloseable owned) {
        this.dataSource = Objects.requireNonNull(dataSource);
        this.dialect = Objects.requireNonNull(dialect);
        this.owned = owned;
    }

    /** {@link #open(String, int)} with {@link #DEFAULT_POOL_SIZE} connections. */
    public static SqlKvStore open(String jdbcUrl) {
        return open(jdbcUrl, DEFAULT_POOL_SIZE);
    }

    /**
     * Opens a pool of {@code poolSize} connections to {@code jdbcUrl} ({@code jdbc:mysql://host:port/db}
     * or {@code jdbc:postgresql://host:port/db}), closed by {@link #close}. The URL may carry
     * {@code user} / {@code password} parameters; otherwise the user is {@code root} (MySQL) or
     * {@code acta} (PG) with no password, as in {@code scripts/bench-dbs.sh}.
     */
    public static SqlKvStore open(String jdbcUrl, int poolSize) {
        Dialect dialect = Dialect.of(jdbcUrl);
        HikariConfig hikari = new HikariConfig();
        hikari.setPoolName("coshim-store-" + jdbcUrl);
        hikari.setJdbcUrl(jdbcUrl);
        if (!jdbcUrl.contains("user=")) {
            hikari.setUsername(dialect.defaultUser);
            hikari.setPassword("");
        }
        hikari.setMaximumPoolSize(poolSize);
        hikari.setMinimumIdle(Math.min(poolSize, 4));
        hikari.setConnectionTimeout(30_000);
        hikari.setAutoCommit(true);
        hikari.setTransactionIsolation("TRANSACTION_READ_COMMITTED");
        if (dialect == Dialect.MYSQL) {
            hikari.setMaxLifetime(300_000);
            // storeAll's batch becomes one multi-row upsert: one round trip per table
            hikari.addDataSourceProperty("rewriteBatchedStatements", "true");
        } else {
            hikari.addDataSourceProperty("reWriteBatchedInserts", "true");
        }
        HikariDataSource pool = new HikariDataSource(hikari);
        return new SqlKvStore(pool, dialect, pool);
    }

    public Dialect dialect() {
        return dialect;
    }

    /** Lets the tests drop the tables they created. */
    DataSource dataSourceForTests() {
        return dataSource;
    }

    @Override
    public String get(TableKey<String> key) {
        String table = table(key);
        return withRetry("get " + key, c -> {
            try (PreparedStatement select = c.prepareStatement(dialect.select(table))) {
                select.setString(1, key.key());
                try (ResultSet rs = select.executeQuery()) {
                    return rs.next() ? rs.getString(1) : null;
                }
            }
        });
    }

    @Override
    public void store(TableKey<String> key, String value) {
        Objects.requireNonNull(value, "values are non-null");
        String table = table(key);
        withRetry("store " + key, c -> {
            try (PreparedStatement upsert = c.prepareStatement(dialect.upsert(table))) {
                upsert.setString(1, key.key());
                upsert.setString(2, value);
                upsert.executeUpdate();
            }
            return null;
        });
    }

    /**
     * One batched upsert per table, on one connection. Keys are sent sorted, so two concurrent
     * batches lock their rows in the same order and cannot deadlock each other inside the database.
     * Not atomic across keys (each table's batch is its own autocommit statement), as {@link
     * KvStore#storeAll} allows; a retry after a partial failure rewrites the same final values.
     */
    @Override
    public void storeAll(Map<TableKey<String>, String> entries) {
        if (entries.isEmpty()) {
            return;
        }
        Map<String, TreeMap<String, String>> byTable = new TreeMap<>();
        entries.forEach((key, value) -> byTable.computeIfAbsent(table(key), t -> new TreeMap<>())
                .put(key.key(), Objects.requireNonNull(value, "values are non-null")));
        withRetry("store " + entries.size() + " entries", c -> {
            for (Map.Entry<String, TreeMap<String, String>> batch : byTable.entrySet()) {
                try (PreparedStatement upsert = c.prepareStatement(dialect.upsert(batch.getKey()))) {
                    for (Map.Entry<String, String> entry : batch.getValue().entrySet()) {
                        upsert.setString(1, entry.getKey());
                        upsert.setString(2, entry.getValue());
                        upsert.addBatch();
                    }
                    upsert.executeBatch();
                }
            }
            return null;
        });
    }

    /** Closes the connection pool if {@link #open} created it. */
    @Override
    public void close() {
        if (owned != null) {
            try {
                owned.close();
            } catch (Exception e) {
                throw new IllegalStateException("closing the connection pool failed", e);
            }
        }
    }

    /** The SQL table of {@code key}, created if this store has not seen it yet. */
    private String table(TableKey<String> key) {
        String table = key.hasTable() ? key.table() : DEFAULT_TABLE;
        if (!tables.contains(table)) {
            if (!IDENTIFIER.matcher(table).matches()) {
                throw new IllegalArgumentException("table names must match " + IDENTIFIER.pattern() + ": '" + table + "'");
            }
            withRetry("create table " + table, c -> {
                try (Statement s = c.createStatement()) {
                    s.execute(dialect.createTable(table));
                } catch (SQLException e) {
                    // PG: two concurrent "create table if not exists" of the same table, the loser
                    // fails on the catalog's unique index; the table exists either way
                    if (!"23505".equals(e.getSQLState()) && !"42P07".equals(e.getSQLState())) {
                        throw e;
                    }
                }
                return null;
            });
            tables.add(table);
        }
        return table;
    }

    private <T> T withRetry(String what, SqlCall<T> call) {
        long backoffMs = 1;
        while (true) {
            try (Connection c = dataSource.getConnection()) {
                return call.run(c);
            } catch (SQLException e) {
                if (!isTransient(e)) {
                    throw new SqlStoreException(what + " failed: " + e.getMessage(), e);
                }
            }
            try {
                Thread.sleep(backoffMs);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(what + ": interrupted while retrying", e);
            }
            backoffMs = Math.min(backoffMs * 2, MAX_BACKOFF_MS);
        }
    }

    /**
     * Lost connections (SQLState class 08), rollbacks by the database (class 40: deadlock,
     * serialization failure) and the driver's / pool's transient exceptions (lock wait timeout,
     * no connection available in time). Batch errors are judged by their cause chain.
     */
    static boolean isTransient(SQLException e) {
        List<Throwable> seen = new ArrayList<>();
        for (Throwable t = e; t != null && !seen.contains(t); t = t.getCause()) {
            seen.add(t);
            if (t instanceof SQLTransientException || t instanceof SQLRecoverableException) {
                return true;
            }
            if (t instanceof SQLException sql) {
                String state = sql.getSQLState();
                if (state != null && (state.startsWith("08") || state.startsWith("40"))) {
                    return true;
                }
                SQLException next = sql.getNextException();
                if (next != null && next != t && !seen.contains(next) && isTransient(next)) {
                    return true;
                }
            }
        }
        return false;
    }
}
