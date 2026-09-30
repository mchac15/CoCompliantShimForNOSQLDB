package ch.epfl.coshim.net;

import java.nio.charset.StandardCharsets;

/** Encodes keys or values for the wire. Must be the same on the node and on its clients. */
public interface Codec<T> {

    byte[] encode(T value);

    T decode(byte[] bytes);

    Codec<String> UTF8 = new Codec<>() {
        @Override
        public byte[] encode(String value) {
            return value.getBytes(StandardCharsets.UTF_8);
        }

        @Override
        public String decode(byte[] bytes) {
            return new String(bytes, StandardCharsets.UTF_8);
        }
    };
}
