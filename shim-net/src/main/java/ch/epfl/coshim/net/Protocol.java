package ch.epfl.coshim.net;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * Wire protocol between the RMs ({@link RemoteCoShim}) and a shim node ({@link CoShimServer}): one
 * request, one response, on a TCP connection that first authenticates.
 *
 * <pre>
 *   handshake   client: int MAGIC, int VERSION, UTF token, UTF database
 *               server: byte HELLO_OK | HELLO_DENIED   (wrong token, unknown database, not allowed)
 *   request     byte op, UTF txnId, [long age], [bytes key], [bytes value]
 *   response    byte OK      + payload (GET, GET_FOR_UPDATE: bytes value, START: boolean, END/PREPARE: byte result)
 *               byte ABORTED + UTF message      (get/put/get-for-update: the shim aborted the txn)
 *               byte ERROR   + UTF message      (unexpected failure on the node)
 *   bytes       int length (-1 = null) + data
 * </pre>
 */
final class Protocol {

    static final int MAGIC = 0xC05A1A;
    static final int VERSION = 4;   // 3: GET_FOR_UPDATE, 4: START_AGED

    static final byte HELLO_OK = 0;
    static final byte HELLO_DENIED = 1;

    static final byte START = 1;
    static final byte GET = 2;
    static final byte PUT = 3;
    static final byte END = 4;
    static final byte PREPARE = 5;
    static final byte COMMIT = 6;
    static final byte ABORT = 7;
    static final byte GET_FOR_UPDATE = 8;   // a get that locks EXCLUSIVE (CoShim.getForUpdate)
    static final byte START_AGED = 9;   // START with the age to give the txn (CoShim.start(txnId, age)); response as START

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
