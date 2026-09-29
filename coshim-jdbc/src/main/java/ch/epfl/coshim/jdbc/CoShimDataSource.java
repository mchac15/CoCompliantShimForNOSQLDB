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
 * <p>Seata reads the URL {@code jdbc:coshim://<name>} for two things: the resource id (the whole URL)
 * and the dbType ({@code coshim}, the JDBC subprotocol). The data source itself never connects to
 * the URL; it sends requests to the {@link CoShim} passed to the constructor. There are two
 * deployments:
 *
 * <ul>
 *   <li><b>Shim as a separate node</b> (the intended one): the CoShim is a
 *       {@code shim-net RemoteCoShim} talking to a {@code CoShimServer}, and the name is the node's
 *       address and store, e.g. {@code "localhost:7000/orders-kv"} in dev. Every application instance
 *       uses the same URL for the same node, exactly like a MySQL URL. That is correct because the TC
 *       routes phase 2 by resource id and may pick any client registered with it, and every client
 *       reaches the same node, which holds the transaction state.</li>
 *   <li><b>In-process shim</b> (tests, single-JVM setups): the CoShim lives in this JVM, so its state is
 *       local. The name must then be unique per instance (e.g. {@code "orders-kv@" + hostname}),
 *       otherwise the TC could route phase 2 to another instance that does not know the txn.</li>
 * </ul>
 */
public class CoShimDataSource<K, V> implements DataSource, XADataSource {

    public static final String URL_PREFIX = "jdbc:coshim://";

    private final String url;
    private final CoShim<K, V> shim;
    private final BranchStates branchStates = new BranchStates();
    private volatile PrintWriter logWriter;
    private volatile int loginTimeout;

    /**
     * @param name the shim's identity for Seata (see the class comment): the node's
     *     {@code "host:port/store"} for a remote shim, or a per-instance name for an in-process one
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
