package ch.epfl.coshim.net;

import ch.epfl.coshim.store.TableKey;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/** Wire encoding of a (table, key) pair: int table length, table (UTF-8), key bytes. */
public final class TableKeyCodec<K> implements Codec<TableKey<K>> {

    private final Codec<K> keyCodec;

    public TableKeyCodec(Codec<K> keyCodec) {
        this.keyCodec = keyCodec;
    }

    @Override
    public byte[] encode(TableKey<K> value) {
        byte[] table = value.table().getBytes(StandardCharsets.UTF_8);
        byte[] key = keyCodec.encode(value.key());
        return ByteBuffer.allocate(Integer.BYTES + table.length + key.length)
                .putInt(table.length)
                .put(table)
                .put(key)
                .array();
    }

    @Override
    public TableKey<K> decode(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        byte[] table = new byte[buffer.getInt()];
        buffer.get(table);
        byte[] key = new byte[buffer.remaining()];
        buffer.get(key);
        return new TableKey<>(new String(table, StandardCharsets.UTF_8), keyCodec.decode(key));
    }
}
