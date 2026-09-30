package ch.epfl.coshim.net;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import ch.epfl.coshim.store.TableKey;
import org.junit.jupiter.api.Test;

class TableKeyCodecTest {

    private final TableKeyCodec<String> codec = new TableKeyCodec<>(Codec.UTF8);

    @Test
    void roundTripsPlainAndTableKeys() {
        assertEquals(TableKey.of("k"), codec.decode(codec.encode(TableKey.of("k"))));
        assertEquals(new TableKey<>("orders", "k"), codec.decode(codec.encode(new TableKey<>("orders", "k"))));
        assertEquals(new TableKey<>("", "k"), codec.decode(codec.encode(new TableKey<>("", "k"))));
    }

    @Test
    void aPlainKeyAndATableKeyAreDifferentKeys() {
        assertNotEquals(TableKey.of("k"), new TableKey<>("orders", "k"));
    }
}
