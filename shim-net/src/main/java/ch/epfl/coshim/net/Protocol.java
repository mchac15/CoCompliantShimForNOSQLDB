package ch.epfl.coshim.net;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * Wire protocol between the RMs ({@link RemoteCoShim}) and a shim node ({@link CoShimServer}): one
 * request, one response, on a TCP connection that first authenticates.
 *
 * <pre>
 *   handshake   client: int MAGIC, int VERSION, UTF token      server: byte HELLO_OK | HELLO_DENIED
 *   request     byte op, UTF txnId, [bytes key], [bytes value]
 *   response    byte OK      + payload (GET: bytes value, START: boolean, END/PREPARE: byte result)
 *               byte ABORTED + UTF message      (get/put: the shim aborted the txn)
 *               byte ERROR   + UTF message      (unexpected failure on the node)
 *   bytes       int length (-1 = null) + data
 * </pre>
 */
final class Protocol {

    static final int MAGIC = 0xC05A1A;
    static final int VERSION = 1;

    static final byte HELLO_OK = 0;
    static final byte HELLO_DENIED = 1;

    static final byte START = 1;
    static final byte GET = 2;
    static final byte PUT = 3;
    static final byte END = 4;
    static final byte PREPARE = 5;
    static final byte COMMIT = 6;
    static final byte ABORT = 7;

    static final byte OK = 0;
    static final byte ABORTED = 1;
    static final byte ERROR = 2;

    private Protocol() {}

    static void writeBytes(DataOutputStream out, byte[] bytes) throws IOException {
        if (bytes == null) {
            out.writeInt(-1);
        } else {
            out.writeInt(bytes.length);
            out.write(bytes);
        }
    }

    static byte[] readBytes(DataInputStream in) throws IOException {
        int length = in.readInt();
        if (length < 0) {
            return null;
        }
        byte[] bytes = new byte[length];
        in.readFully(bytes);
        return bytes;
    }
}
