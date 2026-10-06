package ch.epfl.coshim.store.sql;

import ch.epfl.coshim.store.KvStore;
import ch.epfl.coshim.store.TableKey;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * A MySQL or PostgreSQL database as the shim's {@link KvStore}: {@code get} is a {@code select},
 * {@code store} an upsert, each on its own in autocommit (the shim does the concurrency control).
 *
 * <p>URL: {@code jdbc:coshim:mysql://host:port/db} or {@code jdbc:coshim:pg://host:port/db}, i.e.
 * the database's JDBC URL with {@code coshim:} in front ({@code pg} for {@code postgresql}). It may
 * carry {@code user} / {@code password} parameters; otherwise the user is {@code root} (MySQL) or
 * {@code acta} (PG) with no password, as in the benchmark.
 *
 * <p>All data lives in one table {@value #TABLE} {@code (k, v)}. A {@link TableKey} with a table is
 * stored under {@code table/key}, so the same key in two tables stays two rows. At creation the
 * table is emptied and filled with the plain keys {@code "0" .. entries - 1}, all {@code "0"}.
 */
public final class SqlKvStore implements KvStore<TableKey<String>, String>, AutoCloseable {

    public static final String PREFIX = "jdbc:coshim:";
    public static final String TABLE = "kv";
    private static final int POOL_SIZE = 32;

    enum Dialect {
        MYSQL("mysql", "root", "insert into kv (k, v) values (?, ?) on duplicate key update v = values(v)"),
        POSTGRESQL("postgresql", "acta", "insert into kv (k, v) values (?, ?) on conflict (k) do update set v = excluded.v");

        final String jdbcName;
        final String defaultUser;
        final String upsert;

        Dialect(String jdbcName, String defaultUser, String upsert) {
            this.jdbcName = jdbcName;
            this.defaultUser = defaultUser;
            this.upsert = upsert;
        }
    }

    private final Dialect dialect;
    private final HikariDataSource pool;

    /** Opens {@code url} ({@code jdbc:coshim:mysql:...} or {@code jdbc:coshim:pg:...}) and runs {@link #init}. */
    public SqlKvStore(String url, int entries) {
        if (!url.startsWith(PREFIX)) {
            throw new IllegalArgumentException("not a " + PREFIX + " URL: " + url);
        }
        String rest = url.substring(PREFIX.length());   // e.g. mysql://host:3306/db
        String type = rest.substring(0, Math.max(rest.indexOf(':'), 0));
        dialect = switch (type) {
            case "mysql" -> Dialect.MYSQL;
            case "pg", "postgresql" -> Dialect.POSTGRESQL;
            default -> throw new IllegalArgumentException("unknown database type '" + type + "' (mysql or pg): " + url);
        };
        String jdbcUrl = "jdbc:" + dialect.jdbcName + rest.substring(type.length());

        HikariConfig hikari = new HikariConfig();
        hikari.setPoolName("coshim-store-" + jdbcUrl);
        hikari.setJdbcUrl(jdbcUrl);
        if (!jdbcUrl.contains("user=")) {
            hikari.setUsername(dialect.defaultUser);
            hikari.setPassword("");
        }
        hikari.setMaximumPoolSize(POOL_SIZE);
        hikari.setAutoCommit(true);
        hikari.setTransactionIsolation("TRANSACTION_READ_COMMITTED");
        if (dialect == Dialect.MYSQL) {
            hikari.setMaxLifetime(300_000);
            hikari.addDataSourceProperty("rewriteBatchedStatements", "true");
        } else {
            hikari.addDataSourceProperty("reWriteBatchedInserts", "true");
        }
        pool = new HikariDataSource(hikari);
        init(entries);
    }

    /** Creates {@value #TABLE} if needed, empties it, and fills keys {@code "0" .. entries - 1} with {@code "0"}. */
    private void init(int entries) {
        try (Connection c = pool.getConnection()) {
            try (Statement s = c.createStatement()) {
                s.execute("create table if not exists kv (k varchar(255) not null primary key, v text not null)");
                s.execute(dialect == Dialect.MYSQL ? "truncate table kv" : "truncate kv");
            }
            try (PreparedStatement insert = c.prepareStatement("insert into kv (k, v) values (?, '0')")) {
                for (int k = 0; k < entries; k++) {
                    insert.setString(1, String.valueOf(k));
                    insert.addBatch();
                    if ((k + 1) % 10_000 == 0) {
                        insert.executeBatch();
                    }
                }
                insert.executeBatch();
            }
        } catch (SQLException e) {
            pool.close();
            throw new IllegalStateException("initializing " + TABLE + " failed", e);
        }
    }

    @Override
    public String get(TableKey<String> key) {
        try (Connection c = pool.getConnection();
                PreparedStatement select = c.prepareStatement("select v from kv where k = ?")) {
            select.setString(1, row(key));
            try (ResultSet rs = select.executeQuery()) {
                return rs.next() ? rs.getString(1) : null;
            }
        } catch (SQLException e) {
            throw new IllegalStateException("get " + key + " failed", e);
        }
    }

    @Override
    public void store(TableKey<String> key, String value) {
        try (Connection c = pool.getConnection(); PreparedStatement upsert = c.prepareStatement(dialect.upsert)) {
            upsert.setString(1, row(key));
            upsert.setString(2, value);
            upsert.executeUpdate();
        } catch (SQLException e) {
            throw new IllegalStateException("store " + key + " failed", e);
        }
    }

    @Override
    public void close() {
        pool.close();
    }

    private static String row(TableKey<String> key) {
        return key.hasTable() ? key.table() + "/" + key.key() : key.key();
    }
}
