package ch.epfl.coshim.net;

import ch.epfl.coshim.store.TableKey;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * Wire encoding of an optionally table-qualified key: int table length (-1 = no table), table
 * (UTF-8), key bytes.
 */
public final class TableKeyCodec<K> implements Codec<TableKey<K>> {

    private final Codec<K> keyCodec;

    public TableKeyCodec(Codec<K> keyCodec) {
        this.keyCodec = keyCodec;
    }

    @Override
    public byte[] encode(TableKey<K> value) {
        byte[] table = value.hasTable() ? value.table().getBytes(StandardCharsets.UTF_8) : null;
        byte[] key = keyCodec.encode(value.key());
        ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES + (table == null ? 0 : table.length) + key.length);
        buffer.putInt(table == null ? -1 : table.length);
        if (table != null) {
            buffer.put(table);
        }
        return buffer.put(key).array();
    }

    @Override
    public TableKey<K> decode(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        int tableLength = buffer.getInt();
        String table = null;
        if (tableLength >= 0) {
            byte[] tableBytes = new byte[tableLength];
            buffer.get(tableBytes);
            table = new String(tableBytes, StandardCharsets.UTF_8);
        }
        byte[] key = new byte[buffer.remaining()];
        buffer.get(key);
        return new TableKey<>(table, keyCodec.decode(key));
    }
}
