package ch.epfl.coshim.jdbc;

import ch.epfl.coshim.core.CoShim;
import ch.epfl.coshim.core.TxnAbortedException;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.sql.SQLTransactionRollbackException;

/**
 * A client connection to the shim: the channel over which one client sends its get/put requests.
 * Seata's {@code ConnectionProxyXA} wraps it like a MySQL/PG connection.
 *
 * <p>It has no local transactions: requests are only accepted while an XA branch is associated with
 * it ({@link CoShimXAResource#start} binds the branch's shim txn id, {@code end} unbinds it).
 *
 * <p>One connection carries one branch at a time, exactly like a JDBC connection to any database.
 * That does not serialize transactions: every concurrent transaction (thread / global transaction)
 * gets its own connection from {@link CoShimDataSource#getConnection()} (cheap, no I/O), and all
 * connections share the same shim, whose lock chains order the conflicting ones.
 */
public class CoShimConnection<K, V> extends AbstractUnsupportedConnection implements KvSession<K, V> {

    private final CoShimDataSource<K, V> dataSource;

    /** The branch this connection currently sends requests for, or null between branches. */
    private volatile String txnId;

    /**
     * Only a JDBC flag, never a commit trigger. true is the state of any JDBC connection outside a
     * transaction, and Seata requires it (ConnectionProxyXA.init() rejects connections that start with
     * autoCommit=false). Seata's proxy uses setAutoCommit(false) as "begin": it registers the branch
     * and calls xa start; the proxy handles it and does not forward it here. Nothing is ever committed
     * because of this flag: a branch commits only when the TC sends xa commit in phase 2, after every
     * participant voted YES in xa prepare. A successful prepare is a vote, not a commit.
     */
    private volatile boolean autoCommit = true;

    private volatile boolean closed;

    CoShimConnection(CoShimDataSource<K, V> dataSource) {
        this.dataSource = dataSource;
    }

    CoShimDataSource<K, V> getDataSource() {
        return dataSource;
    }

    void bind(String txnId) {
        this.txnId = txnId;
    }

    /** Unbinds {@code txnId} if it is the branch currently associated. */
    boolean unbind(String txnId) {
        if (txnId.equals(this.txnId)) {
            this.txnId = null;
            return true;
        }
        return false;
    }

    // ---- KvSession ----

    @Override
    public V get(K key) throws SQLException {
        String id = currentTxn();
        try {
            return shim().get(id, key);
        } catch (TxnAbortedException e) {
            throw new SQLTransactionRollbackException(e.getMessage(), e);
        }
    }

    @Override
    public void put(K key, V value) throws SQLException {
        String id = currentTxn();
        try {
            shim().put(id, key, value);
        } catch (TxnAbortedException e) {
            throw new SQLTransactionRollbackException(e.getMessage(), e);
        }
    }

    private CoShim<K, V> shim() {
        return dataSource.getShim();
    }

    /**
     * The branch the next request belongs to. Requests on a connection with no branch are rejected:
     * the shim only runs transactions coordinated by 2PC. This is per connection, not global: other
     * connections run their own branches at the same time.
     */
    private String currentTxn() throws SQLException {
        if (closed) {
            throw new SQLException("connection is closed");
        }
        String id = txnId;
        if (id == null) {
            throw new SQLException("coshim operations need an XA branch: run them inside a global transaction");
        }
        return id;
    }

    // ---- the part of java.sql.Connection Seata and applications use ----

    /** Autocommit is only a flag: XA (driven by Seata) is the only way to run transactions. */
    @Override
    public void setAutoCommit(boolean autoCommit) {
        this.autoCommit = autoCommit;
    }

    @Override
    public boolean getAutoCommit() {
        return autoCommit;
    }

    /** No local transactions: nothing to commit outside XA. */
    @Override
    public void commit() {}

    /**
     * No local transactions. Seata calls this on the physical connection after a failed XA prepare;
     * the shim has already aborted the txn by then.
     */
    @Override
    public void rollback() {}

    @Override
    public void close() {
        closed = true;
    }

    @Override
    public boolean isClosed() {
        return closed;
    }

    @Override
    public boolean isValid(int timeout) {
        return !closed;
    }

    @Override
    public boolean isReadOnly() {
        return false;
    }

    @Override
    public void setReadOnly(boolean readOnly) {}

    @Override
    public int getTransactionIsolation() {
        return Connection.TRANSACTION_SERIALIZABLE;
    }

    @Override
    public java.sql.SQLWarning getWarnings() {
        return null;
    }

    @Override
    public void clearWarnings() {}

    /** Only {@code getURL()} and a few identity methods (Seata derives resourceId and dbType from the URL). */
    @Override
    public DatabaseMetaData getMetaData() {
        return (DatabaseMetaData) Proxy.newProxyInstance(
                DatabaseMetaData.class.getClassLoader(), new Class<?>[] {DatabaseMetaData.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "getURL" -> dataSource.getUrl();
                    case "getDatabaseProductName", "getDriverName" -> "coshim";
                    case "getConnection" -> this;
                    case "isReadOnly" -> false;
                    case "toString" -> "CoShimDatabaseMetaData{" + dataSource.getUrl() + "}";
                    case "hashCode" -> System.identityHashCode(proxy);
                    case "equals" -> proxy == args[0];
                    default -> throw unsupported();
                });
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (iface.isInstance(this)) {
            return iface.cast(this);
        }
        throw new SQLException("not a wrapper for " + iface.getName());
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) {
        return iface.isInstance(this);
    }

    @Override
    public String toString() {
        return "CoShimConnection{" + dataSource.getUrl() + ", txn=" + txnId + "}";
    }
}
