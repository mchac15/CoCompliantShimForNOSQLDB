package ch.epfl.coshim.jdbc;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLTransactionRollbackException;

/**
 * What the application uses instead of SQL: get/put on the tables of the connection's database,
 * inside the transaction the connection is currently in. Obtain it from any connection of a coshim
 * data source, also through Seata's connection proxy: {@code KvSession.<K, V>from(connection)}.
 *
 * <p>If the shim aborts the transaction (lock timeout, cascade, lost upgrade race) the call throws
 * {@link SQLTransactionRollbackException}; the application must stop and roll back (rethrowing out
 * of the global transaction does that).
 */
public interface KvSession<K, V> {

    /**
     * @return the value of {@code key} in {@code table}, or {@code null} if it has none
     * @throws SQLTransactionRollbackException if the transaction must abort
     * @throws SQLException if the connection cannot run the request (closed, or not enlisted yet
     *     inside a global transaction)
     */
    V get(String table, K key) throws SQLException;

    /**
     * @throws SQLTransactionRollbackException if the transaction must abort
     * @throws SQLException if the connection cannot run the request (closed, or not enlisted yet
     *     inside a global transaction)
     */
    void put(String table, K key, V value) throws SQLException;

    @SuppressWarnings("unchecked")
    static <K, V> KvSession<K, V> from(Connection connection) throws SQLException {
        return connection.unwrap(KvSession.class);
    }
}
