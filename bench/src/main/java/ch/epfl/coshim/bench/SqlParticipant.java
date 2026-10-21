package ch.epfl.coshim.bench;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import javax.sql.DataSource;
import org.apache.seata.common.ConfigurationKeys;
import org.apache.seata.common.DefaultValues;
import org.apache.seata.config.ConfigurationFactory;
import org.apache.seata.rm.datasource.util.XAUtils;
import org.apache.seata.rm.datasource.xa.DataSourceProxyXA;

/**
 * A MySQL or PostgreSQL database ({@code jdbc:mysql://host:port/db}, {@code jdbc:postgresql://...}),
 * set up as Acta sets up Sonata's participants: a Hikari pool at SERIALIZABLE (MySQL: S2PL, PG: SSI)
 * wrapped in Seata's stock {@link DataSourceProxyXA}. With {@code sonata.enableGlobalSerializability},
 * Seata's {@code ConnectionProxyXA} adds Sonata's dummy write (and, on PG, a helper txn) before
 * {@code xa prepare}; the rest of the path is the one the shim branches take.
 *
 * <p>Tables follow Acta's Micro: {@code micro-i (key int primary key, value)} with keys
 * 0 .. tableSize - 1, read with {@code select value ... where key=?} and written with
 * {@code update ... set value=? where key=?}. {@code sonata_dummy} is created and filled as Acta's
 * helper does ({@code sonata.dummyTableSize} rows, Sonata's default 1,000,000).
 *
 * <p>The URL may carry {@code user} / {@code password} parameters; otherwise the user is
 * {@code root} (MySQL) or {@code acta} (PG) with no password, as in {@code scripts/bench-dbs.sh}.
 */
final class SqlParticipant implements Participant {

    enum Dialect {
        MYSQL('`', "root"), POSTGRESQL('"', "acta");

        final char quote;
        final String defaultUser;

        Dialect(char quote, String defaultUser) {
            this.quote = quote;
            this.defaultUser = defaultUser;
        }

        String quote(String identifier) {
            return quote + identifier + quote;
        }
    }

    /** Stock {@link DataSourceProxyXA}; only exposes whether Sonata's hook is on for this data source. */
    private static final class SeataProxy extends DataSourceProxyXA {
        SeataProxy(DataSource dataSource) {
            super(dataSource);
        }

        boolean sonataEnabled() {
            return sonataShimEnabled;
        }
    }

    private final String spec;
    private final Dialect dialect;
    private final HikariDataSource pool;
    private final SeataProxy proxy;
    private final boolean sonata;

    SqlParticipant(String spec, BenchConfig config) {
        this.spec = spec;
        if (spec.startsWith("jdbc:mysql:")) {
            dialect = Dialect.MYSQL;
        } else if (spec.startsWith("jdbc:postgresql:")) {
            dialect = Dialect.POSTGRESQL;
        } else {
            throw new IllegalArgumentException("only jdbc:mysql: and jdbc:postgresql: databases are supported: " + spec);
        }
        HikariConfig hikari = new HikariConfig();
        hikari.setPoolName("bench-" + spec);
        hikari.setJdbcUrl(spec);
        if (!spec.contains("user=")) {
            hikari.setUsername(dialect.defaultUser);
            hikari.setPassword("");
        }
        // every branch holds a connection until its phase 2, and with --parallel-branches a worker can
        // have all its branches here: never let the pool become an admission limit (Acta: 1500)
        int size = config.threads() * config.branches() * 2 + 4;
        hikari.setMaximumPoolSize(size);
        hikari.setMinimumIdle(size);
        hikari.setConnectionTimeout(30_000);
        hikari.setTransactionIsolation("TRANSACTION_SERIALIZABLE");
        if (dialect == Dialect.MYSQL) {
            hikari.setMaxLifetime(300_000);
            hikari.addDataSourceProperty("rewriteBatchedStatements", "true");
        } else {
            hikari.addDataSourceProperty("reWriteBatchedInserts", "true");
        }
        pool = new HikariDataSource(hikari);
        proxy = new SeataProxy(pool);
        sonata = config.sonata();
        if (proxy.sonataEnabled() != sonata) {
            pool.close();
            throw new IllegalStateException("Sonata is " + (proxy.sonataEnabled() ? "on" : "off")
                    + " in this JVM (read once, when Seata's XA proxy is loaded) but the run asks for --sonata "
                    + sonata);
        }
    }

    @Override
    public String spec() {
        return spec;
    }

    @Override
    public DataSource dataSource() {
        return proxy;
    }

    @Override
    public void setUp(int tables, int tableSize) throws SQLException {
        try (Connection c = pool.getConnection()) {
            c.setAutoCommit(true);
            rollBackPreparedLeftovers(c);
            for (int t = 0; t < tables; t++) {
                fill(c, MicroBench.table(t), tableSize, "bigint");
            }
            if (sonata) {
                String dummy = ConfigurationFactory.getInstance().getConfig(ConfigurationKeys.SONATA_DUMMY_TABLE,
                        DefaultValues.DEFAULT_SONATA_DUMMY_TABLE);
                int dummySize = ConfigurationFactory.getInstance().getInt(ConfigurationKeys.SONATA_DUMMY_TABLE_SIZE,
                        DefaultValues.DEFAULT_SONATA_DUMMY_TABLE_SIZE);
                fill(c, dummy, dummySize, "int");
            }
            if (dialect == Dialect.MYSQL) {
                // Druid caches MysqlXAConnection.getInstance before making it accessible, so the first
                // concurrent branches race on it and fail: build one XA connection before the workers
                XAUtils.createXAConnection(c.unwrap(Connection.class), proxy);   // as DataSourceProxyXA
            }
        }
    }

    /**
     * A killed run leaves prepared XA branches (and, on PG, Sonata's prepared helper txns) that keep
     * their locks forever; the TC that owned them is gone or no longer tracks them.
     */
    private void rollBackPreparedLeftovers(Connection c) throws SQLException {
        List<String> rollbacks = new ArrayList<>();
        try (Statement s = c.createStatement()) {
            if (dialect == Dialect.MYSQL) {
                try (ResultSet rs = s.executeQuery("xa recover")) {
                    while (rs.next()) {
                        int formatId = rs.getInt("formatID");
                        int gtridLength = rs.getInt("gtrid_length");
                        byte[] data = rs.getBytes("data");
                        HexFormat hex = HexFormat.of();
                        rollbacks.add("xa rollback X'" + hex.formatHex(data, 0, gtridLength) + "',X'"
                                + hex.formatHex(data, gtridLength, data.length) + "'," + formatId);
                    }
                }
            } else {
                try (ResultSet rs = s.executeQuery(
                        "select gid from pg_prepared_xacts where database = current_database()")) {
                    while (rs.next()) {
                        rollbacks.add("rollback prepared '" + rs.getString(1).replace("'", "''") + "'");
                    }
                }
            }
            for (String rollback : rollbacks) {
                s.execute(rollback);
            }
        }
        if (!rollbacks.isEmpty()) {
            System.out.println("  " + spec + ": rolled back " + rollbacks.size() + " prepared txn(s) left by an earlier run");
        }
    }

    /** {@code table} holds exactly keys 0 .. size - 1, all at 0 (Acta's init and populateDummyTable). */
    private void fill(Connection c, String table, int size, String valueType) throws SQLException {
        String t = dialect.quote(table);
        String key = dialect.quote("key");
        try (Statement s = c.createStatement()) {
            s.execute("create table if not exists " + t + " (" + key + " int not null primary key, value "
                    + valueType + " not null)");
            long count;
            long min;
            long max;
            try (ResultSet rs = s.executeQuery("select count(*), coalesce(min(" + key + "), -1), coalesce(max("
                    + key + "), -1) from " + t)) {
                rs.next();
                count = rs.getLong(1);
                min = rs.getLong(2);
                max = rs.getLong(3);
            }
            if (count == size && min == 0 && max == size - 1) {
                s.executeUpdate("update " + t + " set value = 0 where value <> 0");
                return;
            }
            s.execute((dialect == Dialect.MYSQL ? "truncate table " : "truncate ") + t);
        }
        try (PreparedStatement insert = c.prepareStatement("insert into " + t + " (" + key + ", value) values (?, 0)")) {
            for (int k = 0; k < size; k++) {
                insert.setInt(1, k);
                insert.addBatch();
                if ((k + 1) % 10_000 == 0) {
                    insert.executeBatch();
                }
            }
            insert.executeBatch();
        }
    }

    @Override
    public long get(Connection branch, String table, int key) throws SQLException {
        try (PreparedStatement select = branch.prepareStatement(
                "select value from " + dialect.quote(table) + " where " + dialect.quote("key") + " = ?")) {
            select.setInt(1, key);
            try (ResultSet rs = select.executeQuery()) {
                return rs.next() ? rs.getLong(1) : 0;
            }
        }
    }

    @Override
    public void put(Connection branch, String table, int key, long value) throws SQLException {
        try (PreparedStatement update = branch.prepareStatement(
                "update " + dialect.quote(table) + " set value = ? where " + dialect.quote("key") + " = ?")) {
            update.setLong(1, value);
            update.setInt(2, key);
            if (update.executeUpdate() != 1) {
                throw new IllegalStateException(spec + ": key " + key + " of " + table + " is missing");
            }
        }
    }

    @Override
    public long sum(int tables, int tableSize) throws SQLException {
        long sum = 0;
        try (Connection c = pool.getConnection(); Statement s = c.createStatement()) {
            c.setAutoCommit(true);
            for (int t = 0; t < tables; t++) {
                try (ResultSet rs = s.executeQuery("select coalesce(sum(value), 0) from "
                        + dialect.quote(MicroBench.table(t)))) {
                    rs.next();
                    sum += rs.getLong(1);
                }
            }
        }
        return sum;
    }

    @Override
    public void close() {
        if (sonata && dialect == Dialect.POSTGRESQL) {
            try {
                proxy.releaseAllHelpers();   // as Acta's ActaDataSource.close()
            } catch (SQLException | RuntimeException e) {
                System.err.println(spec + ": releasing Sonata's helper txns failed: " + e);
            }
        }
        pool.close();
    }
}
