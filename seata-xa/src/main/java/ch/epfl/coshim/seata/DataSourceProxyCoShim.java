package ch.epfl.coshim.seata;

import ch.epfl.coshim.jdbc.CoShimDataSource;
import java.sql.Connection;
import java.sql.SQLException;
import javax.sql.XAConnection;
import org.apache.seata.rm.datasource.xa.DataSourceProxyXA;

/**
 * Seata XA data-source proxy for a coshim store: the counterpart of {@code new DataSourceProxyXA(mysqlPool)}.
 * Everything is stock Seata XA ({@code ConnectionProxyXA}, {@code ResourceManagerXA}, the TC's
 * {@code XACore}); only the XA connection comes from the coshim driver instead of
 * {@code XAUtils}, which only knows SQL databases.
 *
 * <pre>
 *   DataSource orders = new DataSourceProxyCoShim(new CoShimDataSource&lt;&gt;("orders-kv", shim));
 *   // inside a global transaction:
 *   try (Connection c = orders.getConnection()) {
 *       c.setAutoCommit(false);                          // branchRegister + xa start
 *       KvSession&lt;String, String&gt; kv = KvSession.from(c);
 *       kv.put("order:1", kv.get("stock:7"));
 *       c.commit();
 *   }                                                    // xa end + xa prepare (shim.prepare)
 * </pre>
 *
 * Extending {@link DataSourceProxyXA} (not {@code AbstractDataSourceProxyXA}) matters: the Sonata
 * fork's {@code ConnectionProxyXA} casts its resource to {@code DataSourceProxyXA}. The Sonata hooks
 * stay off for this resource because they are enabled by dbType (mysql/postgresql), and ours is
 * {@code coshim}.
 */
public class DataSourceProxyCoShim extends DataSourceProxyXA {

    private final CoShimDataSource<?, ?> coShimDataSource;

    public DataSourceProxyCoShim(CoShimDataSource<?, ?> dataSource) {
        this(dataSource, DEFAULT_RESOURCE_GROUP_ID);
    }

    public DataSourceProxyCoShim(CoShimDataSource<?, ?> dataSource, String resourceGroupId) {
        super(dataSource, resourceGroupId);
        this.coShimDataSource = dataSource;
    }

    @Override
    protected XAConnection createXAConnection(Connection physicalConn) throws SQLException {
        return coShimDataSource.getXAConnection(physicalConn);
    }
}
