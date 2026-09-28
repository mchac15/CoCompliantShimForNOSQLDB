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
 * plain {@link DataSource} (for Seata's {@code DataSourceProxyXA}) and an {@link XADataSource}. Its
 * URL, {@code jdbc:coshim://<name>}, becomes the Seata resource id and gives dbType {@code coshim}.
 *
 * <p>The shim lives in this JVM: all access to the store must go through this data source.
 */
public class CoShimDataSource<K, V> implements DataSource, XADataSource {

    public static final String URL_PREFIX = "jdbc:coshim://";

    private final String url;
    private final CoShim<K, V> shim;
    private final BranchStates branchStates = new BranchStates();
    private volatile PrintWriter logWriter;
    private volatile int loginTimeout;

    /** @param name unique name of this store among the application's data sources, e.g. "orders-kv" */
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
     * The XA connection for a connection obtained from {@link #getConnection()}: what Seata's
     * {@code DataSourceProxyXA} asks the driver for (see the {@code createXAConnection} hook).
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
