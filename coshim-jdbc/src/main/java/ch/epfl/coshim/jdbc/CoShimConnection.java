package ch.epfl.coshim.jdbc;

import ch.epfl.coshim.core.CoShim;
import ch.epfl.coshim.core.Outcome;
import ch.epfl.coshim.core.TxnAbortedException;
import ch.epfl.coshim.core.Vote;
import ch.epfl.coshim.store.TableKey;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.sql.SQLTransactionRollbackException;
import java.util.UUID;

/**
 * A client connection to the shim: the channel over which one client sends its get/put requests.
 * Seata's {@code ConnectionProxyXA} wraps it like a MySQL/PG connection.
 *
 * <p>Like a JDBC connection to any database, it runs requests in one of two kinds of transaction:
 *
 * <ul>
 *   <li><b>XA branch</b> (inside a Seata global transaction): {@link CoShimXAResource#start} binds the
 *       branch's shim txn id and {@code end} unbinds it; prepare/commit/abort come through XA from the
 *       TC. {@link #commit()} / {@link #rollback()} on this connection do not touch the branch.</li>
 *   <li><b>Local transaction</b> (no global transaction, e.g. Seata hands out the raw connection):
 *       standard JDBC semantics. With autoCommit=true every request is its own transaction; with
 *       autoCommit=false requests accumulate until {@link #commit()} / {@link #rollback()}. The
 *       connection is then the coordinator of a single-participant transaction and drives the shim
 *       itself (end, prepare, commit), so local transactions are ordered with the global ones by the
 *       same lock chains.</li>
 * </ul>
 *
 * <p>One connection carries one transaction at a time, exactly like a JDBC connection to any
 * database. That does not serialize transactions: every concurrent transaction (thread / global
 * transaction) gets its own connection from {@link CoShimDataSource#getConnection()} (cheap, no I/O),
 * and all connections share the same shim, whose lock chains order the conflicting ones.
 */
public class CoShimConnection<K, V> extends AbstractUnsupportedConnection implements KvSession<K, V> {

    private static final String LOCAL_TXN_PREFIX = "local:";

    private final CoShimDataSource<K, V> dataSource;

    /** The XA branch this connection currently sends requests for, or null. */
    private volatile String txnId;

    /** The local transaction in progress (autoCommit=false, no XA branch), or null. */
    private volatile String localTxnId;

    /**
     * JDBC autocommit, for local transactions only. true is the default of any JDBC connection, and
     * Seata requires it (ConnectionProxyXA.init() rejects connections that start with
     * autoCommit=false). Inside a global transaction Seata's proxy handles setAutoCommit(false) itself,
     * as "begin" (branchRegister + xa start), and does not forward it here. So this flag never commits
     * an XA branch: a branch commits only when the TC sends xa commit in phase 2, after every
     * participant voted YES in xa prepare. A successful prepare is a vote, not a commit.
     */
    private volatile boolean autoCommit = true;

    private volatile boolean closed;

    /** See {@link #requireXaBranch()}. */
    private volatile boolean xaBranchRequired;

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

    /**
     * Marks a connection handed out inside a global transaction: its requests must run in an XA
     * branch, never in a local transaction. Otherwise a get/put sent before the branch starts
     * (setAutoCommit(false) forgotten) would silently commit on its own, outside the global transaction.
     * Set by the Seata integration (DataSourceProxyCoShim).
     */
    public void requireXaBranch() {
        this.xaBranchRequired = true;
    }

    /** True while a local transaction is in progress (xa start is then refused, XAER_OUTSIDE). */
    boolean inLocalTransaction() {
        return localTxnId != null;
    }

    // ---- KvSession ----

    @Override
    public V get(String table, K key) throws SQLException {
        TableKey<K> tableKey = new TableKey<>(table, key);
        return request(id -> shim().get(id, tableKey));
    }

    @Override
    public void put(String table, K key, V value) throws SQLException {
        TableKey<K> tableKey = new TableKey<>(table, key);
        request(id -> {
            shim().put(id, tableKey, value);
            return null;
        });
    }

    private CoShim<TableKey<K>, V> shim() {
        return dataSource.getShim();
    }

    private interface Request<R> {
        R send(String txnId);
    }

    /** Sends one request in the connection's current transaction (XA branch, local, or autocommit). */
    private <R> R request(Request<R> request) throws SQLException {
        if (closed) {
            throw new SQLException("connection is closed");
        }
        String branch = txnId;
        if (branch != null) {
            try {
                return request.send(branch);
            } catch (TxnAbortedException e) {
                throw new SQLTransactionRollbackException(e.getMessage(), e);
            }
        }
        if (xaBranchRequired) {
            throw new SQLException("in a global transaction the connection must be enlisted first: "
                    + "call setAutoCommit(false) before sending get/put");
        }
        if (autoCommit) {
            String id = newLocalTxnId();
            try {
                R result = request.send(id);
                commitLocal(id);
                return result;
            } catch (TxnAbortedException e) {
                shim().abort(id);
                throw new SQLTransactionRollbackException(e.getMessage(), e);
            }
        }
        String local = localTxnId;
        if (local == null) {
            local = newLocalTxnId();
            localTxnId = local;
        }
        try {
            return request.send(local);
        } catch (TxnAbortedException e) {
            localTxnId = null;   // the shim aborted it and released its locks
            throw new SQLTransactionRollbackException(e.getMessage(), e);
        }
    }

    private String newLocalTxnId() {
        String id = LOCAL_TXN_PREFIX + UUID.randomUUID();
        shim().start(id);
        return id;
    }

    /** This connection coordinates the single-participant local transaction: end, prepare, commit. */
    private void commitLocal(String id) throws SQLTransactionRollbackException {
        if (shim().end(id) == Outcome.FAILED) {
            throw new SQLTransactionRollbackException("local transaction " + id + " was aborted by the shim");
        }
        if (shim().prepare(id) == Vote.NO) {
            throw new SQLTransactionRollbackException("local transaction " + id + " voted NO");
        }
        shim().commit(id);
    }

    // ---- the part of java.sql.Connection Seata and applications use ----

    /** JDBC: switching autocommit back on commits the local transaction in progress. */
    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        if (autoCommit && !this.autoCommit) {
            commit();
        }
        this.autoCommit = autoCommit;
    }

    @Override
    public boolean getAutoCommit() {
        return autoCommit;
    }

    /** Commits the local transaction, if any. An XA branch is committed by the TC, never here. */
    @Override
    public void commit() throws SQLException {
        String local = localTxnId;
        if (txnId != null || local == null) {
            return;
        }
        localTxnId = null;
        commitLocal(local);
    }

    /**
     * Rolls the local transaction back, if any. An XA branch is rolled back through XA, never here;
     * Seata calls this on the physical connection after a failed xa prepare, when the branch is
     * already unbound and aborted, so it is a no-op then.
     */
    @Override
    public void rollback() {
        String local = localTxnId;
        if (txnId != null || local == null) {
            return;
        }
        localTxnId = null;
        shim().abort(local);
    }

    /** Closing with a local transaction in progress rolls it back. */
    @Override
    public void close() {
        rollback();
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
