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
import java.util.ArrayList;
import java.util.List;

/**
 * A MySQL or PostgreSQL database as the shim's {@link KvStore}, laid out and accessed exactly as the
 * benchmark's Sonata participants ({@code bench/.../SqlParticipant}), so that the two can be compared
 * on the same servers: a table per {@link TableKey} table, {@code (key int primary key, value bigint)},
 * read with {@code select value ... where key = ?} and written with {@code update ... set value = ?
 * where key = ?}. The difference is the concurrency control: here every statement runs on its own in
 * autocommit at READ COMMITTED, since the shim does it; Sonata's run in SERIALIZABLE XA transactions.
 *
 * <p>So keys are integers and values longs, both as decimal strings, and every key has a table.
 *
 * <p>URL: {@code jdbc:coshim:mysql://host:port/db} or {@code jdbc:coshim:pg://host:port/db}, i.e. the
 * database's JDBC URL with {@code coshim:} in front ({@code pg} for {@code postgresql}), with these
 * extra parameters, removed before the URL reaches the driver:
 * <ul>
 *   <li>{@code tables=a,b,...}: created if needed, emptied and filled at creation with keys
 *       {@code 0 .. entries - 1}, all 0 (default: none);</li>
 *   <li>{@code entries=N} (default 0);</li>
 *   <li>{@code pool=N}: connections, all opened up front (default 32).</li>
 * </ul>
 * The other parameters go to the driver. Without {@code user}, the user is {@code root} (MySQL) or
 * {@code acta} (PG) with no password, as in {@code scripts/bench-dbs.sh}.
 */
public final class SqlKvStore implements KvStore<TableKey<String>, String>, AutoCloseable {

    public static final String PREFIX = "jdbc:coshim:";

    enum Dialect {
        MYSQL("mysql", '`', "root"), POSTGRESQL("postgresql", '"', "acta");

        final String jdbcName;
        final char quote;
        final String defaultUser;

        Dialect(String jdbcName, char quote, String defaultUser) {
            this.jdbcName = jdbcName;
            this.quote = quote;
            this.defaultUser = defaultUser;
        }

        String quote(String identifier) {
            return quote + identifier + quote;
        }
    }

    private final Dialect dialect;
    private final HikariDataSource pool;

    public SqlKvStore(String url) {
        if (!url.startsWith(PREFIX)) {
            throw new IllegalArgumentException("not a " + PREFIX + " URL: " + url);
        }
        String rest = url.substring(PREFIX.length());   // e.g. mysql://host:3306/db?tables=...
        String type = rest.substring(0, Math.max(rest.indexOf(':'), 0));
        dialect = switch (type) {
            case "mysql" -> Dialect.MYSQL;
            case "pg", "postgresql" -> Dialect.POSTGRESQL;
            default -> throw new IllegalArgumentException("unknown database type '" + type + "' (mysql or pg): " + url);
        };
        rest = rest.substring(type.length());

        // split off our parameters; the rest of the query goes to the driver
        List<String> tables = List.of();
        int entries = 0;
        int poolSize = 32;
        int query = rest.indexOf('?');
        List<String> driverParams = new ArrayList<>();
        if (query >= 0) {
            for (String param : rest.substring(query + 1).split("&")) {
                String name = param.substring(0, Math.max(param.indexOf('='), 0));
                String value = param.substring(param.indexOf('=') + 1);
                switch (name) {
                    case "tables" -> tables = value.isEmpty() ? List.of() : List.of(value.split(","));
                    case "entries" -> entries = Integer.parseInt(value);
                    case "pool" -> poolSize = Integer.parseInt(value);
                    default -> driverParams.add(param);
                }
            }
            rest = rest.substring(0, query);
        }
        String jdbcUrl = "jdbc:" + dialect.jdbcName + rest
                + (driverParams.isEmpty() ? "" : "?" + String.join("&", driverParams));

        // as SqlParticipant, except the isolation level
        HikariConfig hikari = new HikariConfig();
        hikari.setPoolName("coshim-store-" + jdbcUrl);
        hikari.setJdbcUrl(jdbcUrl);
        if (!jdbcUrl.contains("user=")) {
            hikari.setUsername(dialect.defaultUser);
            hikari.setPassword("");
        }
        hikari.setMaximumPoolSize(poolSize);
        hikari.setMinimumIdle(poolSize);
        hikari.setConnectionTimeout(30_000);
        hikari.setAutoCommit(true);
        hikari.setTransactionIsolation("TRANSACTION_READ_COMMITTED");
        if (dialect == Dialect.MYSQL) {
            hikari.setMaxLifetime(300_000);
            hikari.addDataSourceProperty("rewriteBatchedStatements", "true");
        } else {
            hikari.addDataSourceProperty("reWriteBatchedInserts", "true");
        }
        pool = new HikariDataSource(hikari);
        try {
            for (String table : tables) {
                init(table, entries);
            }
        } catch (SQLException | RuntimeException e) {
            pool.close();
            throw new IllegalStateException("initializing the tables of " + jdbcUrl + " failed", e);
        }
    }

    /** {@code table} holds exactly keys 0 .. entries - 1, all at 0 (SqlParticipant.fill). */
    private void init(String table, int entries) throws SQLException {
        String t = dialect.quote(table);
        String key = dialect.quote("key");
        try (Connection c = pool.getConnection()) {
            try (Statement s = c.createStatement()) {
                s.execute("create table if not exists " + t + " (" + key + " int not null primary key, value bigint not null)");
                s.execute((dialect == Dialect.MYSQL ? "truncate table " : "truncate ") + t);
            }
            try (PreparedStatement insert = c.prepareStatement("insert into " + t + " (" + key + ", value) values (?, 0)")) {
                for (int k = 0; k < entries; k++) {
                    insert.setInt(1, k);
                    insert.addBatch();
                    if ((k + 1) % 10_000 == 0) {
                        insert.executeBatch();
                    }
                }
                insert.executeBatch();
            }
        }
    }

    @Override
    public String get(TableKey<String> key) {
        try (Connection c = pool.getConnection();
                PreparedStatement select = c.prepareStatement(
                        "select value from " + table(key) + " where " + dialect.quote("key") + " = ?")) {
            select.setInt(1, Integer.parseInt(key.key()));
            try (ResultSet rs = select.executeQuery()) {
                return rs.next() ? String.valueOf(rs.getLong(1)) : null;
            }
        } catch (SQLException e) {
            throw new IllegalStateException("get " + key + " failed", e);
        }
    }

    /** {@code update}, as Sonata's participants; a key outside the filled range is inserted. */
    @Override
    public void store(TableKey<String> key, String value) {
        String t = table(key);
        String k = dialect.quote("key");
        try (Connection c = pool.getConnection()) {
            try (PreparedStatement update = c.prepareStatement("update " + t + " set value = ? where " + k + " = ?")) {
                update.setLong(1, Long.parseLong(value));
                update.setInt(2, Integer.parseInt(key.key()));
                if (update.executeUpdate() == 1) {
                    return;
                }
            }
            try (PreparedStatement insert = c.prepareStatement("insert into " + t + " (" + k + ", value) values (?, ?)")) {
                insert.setInt(1, Integer.parseInt(key.key()));
                insert.setLong(2, Long.parseLong(value));
                insert.executeUpdate();
            }
        } catch (SQLException e) {
            throw new IllegalStateException("store " + key + " failed", e);
        }
    }

    @Override
    public void close() {
        pool.close();
    }

    private String table(TableKey<String> key) {
        if (!key.hasTable()) {
            throw new IllegalArgumentException("SqlKvStore keys need a table: " + key);
        }
        return dialect.quote(key.table());
    }
}
