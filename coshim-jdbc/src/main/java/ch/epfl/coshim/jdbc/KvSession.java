package ch.epfl.coshim.jdbc;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLTransactionRollbackException;

/**
 * What the application uses instead of SQL: get/put on the shim, inside the XA branch the
 * connection is currently enlisted in. Obtain it from any connection of a coshim data source, also
 * through Seata's connection proxy: {@code KvSession.<K, V>from(connection)}.
 *
 * <p>If the shim aborts the transaction (lock timeout, cascade, lost upgrade race) the call throws
 * {@link SQLTransactionRollbackException}; the application must stop and roll back (rethrowing out
 * of the global transaction does that).
 */
public interface KvSession<K, V> {

    /**
     * @return the value, or {@code null} if the key has none
     * @throws SQLTransactionRollbackException if the transaction must abort
     * @throws SQLException if the connection is not in an XA branch (no global transaction)
     */
    V get(K key) throws SQLException;

    /**
     * @throws SQLTransactionRollbackException if the transaction must abort
     * @throws SQLException if the connection is not in an XA branch (no global transaction)
     */
    void put(K key, V value) throws SQLException;

    @SuppressWarnings("unchecked")
    static <K, V> KvSession<K, V> from(Connection connection) throws SQLException {
        return connection.unwrap(KvSession.class);
    }
}
