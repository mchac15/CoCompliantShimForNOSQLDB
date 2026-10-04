package ch.epfl.coshim.jdbc;

import java.sql.Connection;
import javax.sql.ConnectionEventListener;
import javax.sql.StatementEventListener;
import javax.sql.XAConnection;
import javax.transaction.xa.XAResource;

/** Pairs a {@link CoShimConnection} with the {@link CoShimXAResource} that enlists it in XA branches. */
public class CoShimXAConnection implements XAConnection {

    private final CoShimConnection<?, ?> connection;
    private final CoShimXAResource xaResource;

    CoShimXAConnection(CoShimConnection<?, ?> connection) {
        this.connection = connection;
        this.xaResource = new CoShimXAResource(connection);
    }

    @Override
    public XAResource getXAResource() {
        return xaResource;
    }

    @Override
    public Connection getConnection() {
        return connection;
    }

    @Override
    public void close() {
        connection.close();
    }

    @Override
    public void addConnectionEventListener(ConnectionEventListener listener) {}

    @Override
    public void removeConnectionEventListener(ConnectionEventListener listener) {}

    @Override
    public void addStatementEventListener(StatementEventListener listener) {}

    @Override
    public void removeStatementEventListener(StatementEventListener listener) {}
}
