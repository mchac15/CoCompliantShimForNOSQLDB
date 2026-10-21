package ch.epfl.coshim.bench;

import java.sql.Connection;
import java.sql.SQLException;
import javax.sql.DataSource;

/**
 * One XA participant of the benchmark, as the application sees it: a Seata-proxied data source whose
 * connections become branches, plus how a branch reads and writes a Micro key on it. Behind it is
 * either a shim database ({@link CoShimParticipant}) or a MySQL/PostgreSQL database
 * ({@link SqlParticipant}), on which Sonata runs when {@code sonata.enableGlobalSerializability} is
 * set. The rest of the benchmark (TM, TC, attempts, audit) is the same for both.
 */
sealed interface Participant extends AutoCloseable permits CoShimParticipant, SqlParticipant {

    /**
     * {@code host:port/database} is a shim database; {@code jdbc:mysql://...} and
     * {@code jdbc:postgresql://...} are SQL databases (Sonata's participants).
     *
     * @param token the shim nodes' shared secret, only used by shim databases
     */
    static Participant connect(String spec, String token, BenchConfig config) {
        if (spec.startsWith("jdbc:")) {
            return new SqlParticipant(spec, config);
        }
        if (token == null || token.isEmpty()) {
            throw new IllegalArgumentException("set COSHIM_TOKEN to the shim nodes' token");
        }
        return new CoShimParticipant(spec, token);
    }

    /** Whether {@code spec} names a shim database, which needs COSHIM_TOKEN. */
    static boolean isCoShim(String spec) {
        return !spec.startsWith("jdbc:");
    }

    /** As given on the command line. */
    String spec();

    /** The Seata XA data source: a connection with {@code setAutoCommit(false)} is a branch. */
    DataSource dataSource();

    /**
     * Before the run: every table micro-0 .. micro-(tables - 1) holds keys 0 .. tableSize - 1 at 0
     * (a missing key counts as 0).
     */
    void setUp(int tables, int tableSize) throws SQLException;

    /** Reads a key inside a branch; a missing key is 0. */
    long get(Connection branch, String table, int key) throws SQLException;

    /** Writes a key inside a branch. */
    void put(Connection branch, String table, int key, long value) throws SQLException;

    /** The audit: sum of every value of micro-0 .. micro-(tables - 1), outside any global transaction. */
    long sum(int tables, int tableSize) throws SQLException;

    @Override
    void close();
}
