package ch.epfl.coshim.net;

import ch.epfl.coshim.core.CoShim;
import ch.epfl.coshim.core.Outcome;
import ch.epfl.coshim.core.TxnAbortedException;
import ch.epfl.coshim.core.Vote;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Objects;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Client side of a shim node, used by the RMs: a {@link CoShim} whose requests go over TCP to a
 * {@link CoShimServer}.
 *
 * <p>A request occupies one socket until its response arrives, and requests can block for a long
 * time (lock waits, prepare). So the client keeps a pool: a request borrows an idle authenticated
 * socket or opens a new one, and returns it afterwards. Concurrent transactions therefore never
 * wait on each other's sockets. A request is not tied to a socket: the node identifies the
 * transaction by its txn id.
 *
 * <p>A socket that fails mid-request is discarded and the failure surfaces as
 * {@link UncheckedIOException}. There is no automatic retry, since a request may already have
 * taken effect on the node. The XA layer turns it into a failed branch.
 */
public class RemoteCoShim<K, V> implements CoShim<K, V>, AutoCloseable {

    private final InetSocketAddress node;
    private final String database;
    private final String token;
    private final Codec<K> keyCodec;
    private final Codec<V> valueCodec;
    private final ConcurrentLinkedQueue<Connection> idle = new ConcurrentLinkedQueue<>();
    private volatile boolean closed;

    /**
     * @param node the shim node
     * @param database the database on that node; every request of this client goes to its shim
     */
    public RemoteCoShim(
            InetSocketAddress node, String database, String token, Codec<K> keyCodec, Codec<V> valueCodec) {
        this.node = Objects.requireNonNull(node);
        this.database = Objects.requireNonNull(database);
        this.token = Objects.requireNonNull(token);
        this.keyCodec = Objects.requireNonNull(keyCodec);
        this.valueCodec = Objects.requireNonNull(valueCodec);
    }

    /** Opens and authenticates one connection now, to fail fast on a wrong address or token. */
    public RemoteCoShim<K, V> verify() {
        idle.add(open());
        return this;
    }

    @Override
    public boolean start(String txnId) {
        return call(Protocol.START, txnId, null, null, in -> in.readBoolean());
    }

    @Override
    public V get(String txnId, K key) {
        return call(Protocol.GET, txnId, key, null, in -> {
            byte[] bytes = Protocol.readBytes(in);
            return bytes == null ? null : valueCodec.decode(bytes);
        });
    }

    @Override
    public void put(String txnId, K key, V value) {
        call(Protocol.PUT, txnId, key, value, in -> null);
    }

    @Override
    public Outcome end(String txnId) {
        return call(Protocol.END, txnId, null, null, in -> Outcome.values()[in.readByte()]);
    }

    @Override
    public Vote prepare(String txnId) {
        return call(Protocol.PREPARE, txnId, null, null, in -> Vote.values()[in.readByte()]);
    }

    @Override
    public void commit(String txnId) {
        call(Protocol.COMMIT, txnId, null, null, in -> null);
    }

    @Override
    public void abort(String txnId) {
        call(Protocol.ABORT, txnId, null, null, in -> null);
    }

    private interface Reader<R> {
        R read(DataInputStream in) throws IOException;
    }

    private <R> R call(byte op, String txnId, K key, V value, Reader<R> payload) {
        if (closed) {
            throw new IllegalStateException("closed");
        }
        Connection c = idle.poll();
        if (c == null) {
            c = open();
        }
        try {
            c.out.writeByte(op);
            c.out.writeUTF(txnId);
            if (op == Protocol.GET || op == Protocol.PUT) {
                Protocol.writeBytes(c.out, keyCodec.encode(key));
            }
            if (op == Protocol.PUT) {
                Protocol.writeBytes(c.out, valueCodec.encode(value));
            }
            c.out.flush();
            byte status = c.in.readByte();
            R result;
            if (status == Protocol.OK) {
                result = payload.read(c.in);
            } else if (status == Protocol.ABORTED) {
                String message = c.in.readUTF();
                idle.add(c);
                throw new TxnAbortedException(message);
            } else {
                String message = c.in.readUTF();
                idle.add(c);
                throw new IllegalStateException("shim node " + node + ": " + message);
            }
            idle.add(c);
            return result;
        } catch (IOException e) {
            c.closeQuietly();
            throw new UncheckedIOException("shim node " + node + " unreachable", e);
        }
    }

    private Connection open() {
        try {
            Socket socket = new Socket();
            socket.connect(node);
            socket.setTcpNoDelay(true);
            Connection c = new Connection(socket);
            c.out.writeInt(Protocol.MAGIC);
            c.out.writeInt(Protocol.VERSION);
            c.out.writeUTF(token);
            c.out.writeUTF(database);
            c.out.flush();
            if (c.in.readByte() != Protocol.HELLO_OK) {
                c.closeQuietly();
                throw new SecurityException("shim node " + node + " rejected the connection to database '" + database
                        + "' (wrong token, unknown database, or client not allowed)");
            }
            return c;
        } catch (IOException e) {
            throw new UncheckedIOException("cannot connect to shim node " + node, e);
        }
    }

    @Override
    public void close() {
        closed = true;
        Connection c;
        while ((c = idle.poll()) != null) {
            c.closeQuietly();
        }
    }

    private static final class Connection {
        final Socket socket;
        final DataInputStream in;
        final DataOutputStream out;

        Connection(Socket socket) throws IOException {
            this.socket = socket;
            this.in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
            this.out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
        }

        void closeQuietly() {
            try {
                socket.close();
            } catch (IOException ignored) {
                // already broken
            }
        }
    }
}
