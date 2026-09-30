package ch.epfl.coshim.jdbc;

import ch.epfl.coshim.core.CoShim;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Objects;
import java.util.logging.Logger;
import javax.sql.DataSource;
import javax.sql.XADataSource;

/**
 * A {@link CoShim} exposed as a JDBC data source, the way a MySQL/PG driver exposes a database: a
 * plain {@link DataSource} (for Seata's {@code DataSourceProxyXA}) and an {@link XADataSource}.
 *
 * <p>The URL {@code jdbc:coshim://<name>} is an <b>identity, not a network address</b>. Nothing
 * connects to it: the shim is the {@link CoShim} object passed to the constructor, in this JVM, so
 * there is no host or port to configure, in dev (localhost) or in production. Seata reads the URL for
 * two things only: the resource id (the whole URL) and the dbType ({@code coshim}, the JDBC
 * subprotocol).
 *
 * <p>The resource id is what the TC uses to route phase 2 (xa commit/rollback) back to an RM, and it
 * falls back to any client registered with the same id. For MySQL/PG any client can finish a branch,
 * because the database keeps the XA state. Here the state lives in this JVM's shim, so the name must
 * be unique per shim instance: two application instances must not register the same
 * {@code jdbc:coshim://<name>} with different in-process shims. Deriving it from the store and the
 * instance (e.g. {@code "orders-kv@" + hostname}) is enough.
 *
 * <p>If the shim later becomes a separate server shared by several application instances, the URL
 * becomes a real address, e.g. {@code jdbc:coshim://localhost:7000/orders-kv} in dev. The same id
 * then designates the same shared shim for every client, exactly like a MySQL URL. dbType parsing
 * does not change, because it only looks at the subprotocol.
 */
public class CoShimDataSource<K, V> implements DataSource, XADataSource {

    public static final String URL_PREFIX = "jdbc:coshim://";

    private final String url;
    private final CoShim<K, V> shim;
    private final BranchStates branchStates = new BranchStates();
    private volatile PrintWriter logWriter;
    private volatile int loginTimeout;

    /**
     * @param name identity of this shim instance, unique across every RM registered with the TC
     *     (see the class comment), e.g. "orders-kv@app-1"
     */
    public CoShimDataSource(String name, CoShim<K, V> shim) {
        this.url = URL_PREFIX + Objects.requireNonNull(name);
        this.shim = Objects.requireNonNull(shim);
    }

    public String getUrl() {
        return url;
    }

    public CoShim<K, V> getShim() {
        return shim;
    }

    BranchStates getBranchStates() {
        return branchStates;
    }

    @Override
    public CoShimConnection<K, V> getConnection() {
        return new CoShimConnection<>(this);
    }

    @Override
    public Connection getConnection(String username, String password) {
        return getConnection();
    }

    @Override
    public CoShimXAConnection getXAConnection() {
        return new CoShimXAConnection(getConnection());
    }

    @Override
    public CoShimXAConnection getXAConnection(String user, String password) {
        return getXAConnection();
    }

    /**
     * Wraps a connection previously obtained from {@link #getConnection()} into an XA connection, so
     * that XA calls (start/end/prepare/commit/rollback) can be issued for it.
     *
     * <p>Why this exists: Seata's {@code DataSourceProxyXA} does not ask the driver for an XA
     * connection directly. It first takes a plain connection from the wrapped data source, then turns
     * it into an XA connection. For MySQL/PG it does that with {@code XAUtils.createXAConnection(conn,
     * dbType)}, which knows only SQL drivers. Our patch adds an overridable hook
     * ({@code createXAConnection}) and {@code DataSourceProxyCoShim} overrides it to call this method.
     * The XA connection wraps the same {@link CoShimConnection} the application uses, so the
     * {@link CoShimXAResource} binds branches to the connection that actually sends the get/put
     * requests. The ownership check keeps a connection of another coshim data source (another shim)
     * from being enlisted here by mistake.
     */
    public CoShimXAConnection getXAConnection(Connection physicalConnection) throws SQLException {
        CoShimConnection<?, ?> connection = physicalConnection.unwrap(CoShimConnection.class);
        if (connection.getDataSource() != this) {
            throw new SQLException(physicalConnection + " does not belong to " + url);
        }
        return new CoShimXAConnection(connection);
    }

    @Override
    public PrintWriter getLogWriter() {
        return logWriter;
    }

    @Override
    public void setLogWriter(PrintWriter out) {
        this.logWriter = out;
    }

    @Override
    public void setLoginTimeout(int seconds) {
        this.loginTimeout = seconds;
    }

    @Override
    public int getLoginTimeout() {
        return loginTimeout;
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException();
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
        return "CoShimDataSource{" + url + "}";
    }
}
