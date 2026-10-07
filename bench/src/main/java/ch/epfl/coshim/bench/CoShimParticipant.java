package ch.epfl.coshim.bench;

import ch.epfl.coshim.jdbc.CoShimDataSource;
import ch.epfl.coshim.jdbc.KvSession;
import ch.epfl.coshim.net.Codec;
import ch.epfl.coshim.net.RemoteCoShim;
import ch.epfl.coshim.net.TableKeyCodec;
import ch.epfl.coshim.seata.DataSourceProxyCoShim;
import ch.epfl.coshim.store.TableKey;
import java.net.InetSocketAddress;
import java.sql.Connection;
import java.sql.SQLException;
import javax.sql.DataSource;

/** A shim database ({@code host:port/database}) in a shim node, reached over TCP. */
final class CoShimParticipant implements Participant {

    private final String spec;
    private final RemoteCoShim<TableKey<String>, String> remote;
    private final DataSource dataSource;

    CoShimParticipant(String spec, String token) {
        this.spec = spec;
        int colon = spec.indexOf(':');
        int slash = spec.indexOf('/');
        InetSocketAddress node = new InetSocketAddress(spec.substring(0, colon),
                Integer.parseInt(spec.substring(colon + 1, slash)));
        remote = new RemoteCoShim<>(node, spec.substring(slash + 1), token, new TableKeyCodec<>(Codec.UTF8),
                Codec.UTF8).verify();
        dataSource = new DataSourceProxyCoShim(new CoShimDataSource<>(spec, remote));
    }

    @Override
    public String spec() {
        return spec;
    }

    @Override
    public DataSource dataSource() {
        return dataSource;
    }

    /** Nothing to do: the script starts fresh nodes for every run, whose stores are empty. */
    @Override
    public void setUp(int tables, int tableSize) {
    }

    @Override
    public long get(Connection branch, String table, int key) throws SQLException {
        String v = KvSession.<String, String>from(branch).get(table, String.valueOf(key));
        return v == null ? 0 : Long.parseLong(v);
    }

    @Override
    public void put(Connection branch, String table, int key, long value) throws SQLException {
        KvSession.<String, String>from(branch).put(table, String.valueOf(key), String.valueOf(value));
    }

    @Override
    public long sum(int tables, int tableSize) throws SQLException {
        long sum = 0;
        try (Connection c = dataSource.getConnection()) {
            for (int t = 0; t < tables; t++) {
                for (int k = 0; k < tableSize; k++) {
                    sum += get(c, MicroBench.table(t), k);
                }
            }
        }
        return sum;
    }

    @Override
    public void close() {
        remote.close();
    }
}
