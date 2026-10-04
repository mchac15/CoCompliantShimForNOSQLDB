package ch.epfl.coshim.net;

import ch.epfl.coshim.core.CoShim;
import ch.epfl.coshim.core.Outcome;
import ch.epfl.coshim.core.TxnAbortedException;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A shim node, served over TCP to the transaction participants, i.e. the Seata RMs running the coshim
 * JDBC driver ({@link RemoteCoShim}). A node hosts one or more databases, and there is exactly one
 * {@link CoShim} per database: a data source is (this node, a database name). A connection selects
 * its database in the handshake and is bound to that database's shim for its lifetime, so databases
 * are fully separate (their own lock chains and transactions). How many databases a node hosts is a
 * deployment choice: one node per database, or several databases on one server.
 *
 * <p>Access: the node accepts many concurrent connections, but only from participants. Every
 * connection must authenticate with the node's shared token, compared in constant time, before
 * any request is served. It can also be restricted to an allowlist of client addresses. Everyone
 * else is rejected. The Seata TC itself never connects: as for MySQL/PG, it only talks to the RMs,
 * which forward xa commit/rollback here.
 *
 * <p>Concurrency: each connection is served by its own thread and handles one request at a time.
 * A request that blocks in the shim (a lock wait in get/put, prepare waiting for predecessors)
 * blocks only its connection. Other transactions keep running on other connections, and the shim's
 * shared state orders the conflicting ones.
 */
public class CoShimServer<K, V> implements AutoCloseable {

    private static final Logger LOG = Logger.getLogger(CoShimServer.class.getName());

    private final Map<String, CoShim<K, V>> databases;
    private final Codec<K> keyCodec;
    private final Codec<V> valueCodec;
    private final byte[] token;
    private final Set<InetAddress> allowedClients;
    private final ServerSocket serverSocket;
    private final ExecutorService connections = Executors.newCachedThreadPool(r -> {
        Thread t = new Thread(r, "coshim-conn");
        t.setDaemon(true);
        return t;
    });
    private volatile boolean closed;

    /**
     * @param databases database name -> the shim of that database
     * @param bind address to listen on; port 0 picks a free port (see {@link #getPort()})
     * @param token shared secret the participants must present; must not be empty
     * @param allowedClients client addresses allowed to connect, or empty for any authenticated client
     */
    public CoShimServer(
            Map<String, ? extends CoShim<K, V>> databases,
            Codec<K> keyCodec,
            Codec<V> valueCodec,
            InetSocketAddress bind,
            String token,
            Set<InetAddress> allowedClients)
            throws IOException {
        this.databases = Map.copyOf(databases);
        this.keyCodec = Objects.requireNonNull(keyCodec);
        this.valueCodec = Objects.requireNonNull(valueCodec);
        if (token == null || token.isEmpty()) {
            throw new IllegalArgumentException("a shim node needs an authentication token");
        }
        this.token = token.getBytes(StandardCharsets.UTF_8);
        this.allowedClients = Set.copyOf(allowedClients);
        this.serverSocket = new ServerSocket();
        serverSocket.bind(bind);
        Thread acceptor = new Thread(this::acceptLoop, "coshim-accept");
        acceptor.setDaemon(true);
        acceptor.start();
    }

    public int getPort() {
        return serverSocket.getLocalPort();
    }

    private void acceptLoop() {
        while (!closed) {
            try {
                Socket socket = serverSocket.accept();
                connections.execute(() -> serve(socket));
            } catch (IOException e) {
                if (!closed) {
                    LOG.log(Level.WARNING, "accept failed", e);
                }
            }
        }
    }

    private void serve(Socket socket) {
        try (socket) {
            socket.setTcpNoDelay(true);
            DataInputStream in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
            DataOutputStream out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
            CoShim<K, V> shim = authenticate(socket, in);
            if (shim == null) {
                out.writeByte(Protocol.HELLO_DENIED);
                out.flush();
                LOG.warning("rejected connection from " + socket.getRemoteSocketAddress());
                return;
            }
            out.writeByte(Protocol.HELLO_OK);
            out.flush();
            while (!closed) {
                byte op;
                try {
                    op = in.readByte();
                } catch (EOFException clientClosed) {
                    return;
                }
                handle(shim, op, in, out);
                out.flush();
            }
        } catch (IOException e) {
            if (!closed) {
                LOG.log(Level.FINE, "connection " + socket.getRemoteSocketAddress() + " closed", e);
            }
        }
    }

    /** @return the shim of the database the client asked for, or null if the client is rejected */
    private CoShim<K, V> authenticate(Socket socket, DataInputStream in) throws IOException {
        if (!allowedClients.isEmpty() && !allowedClients.contains(socket.getInetAddress())) {
            return null;
        }
        if (in.readInt() != Protocol.MAGIC || in.readInt() != Protocol.VERSION) {
            return null;
        }
        byte[] presented = in.readUTF().getBytes(StandardCharsets.UTF_8);
        String database = in.readUTF();
        if (!MessageDigest.isEqual(presented, token)) {
            return null;
        }
        return databases.get(database);   // unknown database: rejected too
    }

    private void handle(CoShim<K, V> shim, byte op, DataInputStream in, DataOutputStream out) throws IOException {
        String txnId = in.readUTF();
        K key = null;
        V value = null;
        if (op == Protocol.GET || op == Protocol.PUT) {
            key = keyCodec.decode(Protocol.readBytes(in));
        }
        if (op == Protocol.PUT) {
            value = valueCodec.decode(Protocol.readBytes(in));
        }
        try {
            switch (op) {
                case Protocol.START -> {
                    boolean started = shim.start(txnId) == Outcome.SUCCEEDED;
                    out.writeByte(Protocol.OK);
                    out.writeBoolean(started);
                }
                case Protocol.GET -> {
                    V result = shim.get(txnId, key);
                    out.writeByte(Protocol.OK);
                    Protocol.writeBytes(out, result == null ? null : valueCodec.encode(result));
                }
                case Protocol.PUT -> {
                    shim.put(txnId, key, value);
                    out.writeByte(Protocol.OK);
                }
                case Protocol.END -> {
                    out.writeByte(Protocol.OK);
                    out.writeByte(shim.end(txnId).ordinal());
                }
                case Protocol.PREPARE -> {
                    out.writeByte(Protocol.OK);
                    out.writeByte(shim.prepare(txnId).ordinal());
                }
                case Protocol.COMMIT -> {
                    shim.commit(txnId);
                    out.writeByte(Protocol.OK);
                }
                case Protocol.ABORT -> {
                    shim.abort(txnId);
                    out.writeByte(Protocol.OK);
                }
                default -> throw new IOException("unknown op " + op);
            }
        } catch (TxnAbortedException e) {
            out.writeByte(Protocol.ABORTED);
            out.writeUTF(String.valueOf(e.getMessage()));
        } catch (RuntimeException e) {
            LOG.log(Level.WARNING, "request " + op + " for " + txnId + " failed", e);
            out.writeByte(Protocol.ERROR);
            out.writeUTF(e.toString());
        }
    }

    @Override
    public void close() throws IOException {
        closed = true;
        serverSocket.close();
        connections.shutdownNow();
    }
}
