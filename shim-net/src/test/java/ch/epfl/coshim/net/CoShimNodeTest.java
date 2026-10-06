package ch.epfl.coshim.net;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

class CoShimNodeTest {

    @Test
    void storeIsInMemoryByDefault() throws Exception {
        CoShimNode.Options options = CoShimNode.Options.parse(new String[] {"--databases", "a,b"});
        assertNull(options.storeUrl("a"));
    }

    @Test
    void eachDatabaseGetsItsOwnSqlDatabase() throws Exception {
        CoShimNode.Options options = CoShimNode.Options.parse(new String[] {
            "--databases", "shop,billing", "--store", "jdbc:postgresql://db:5432/{database}?user=u"});
        assertEquals("jdbc:postgresql://db:5432/shop?user=u", options.storeUrl("shop"));
        assertEquals("jdbc:postgresql://db:5432/billing?user=u", options.storeUrl("billing"));
    }

    @Test
    void oneDatabaseMayUseAFixedUrl() throws Exception {
        CoShimNode.Options options = CoShimNode.Options.parse(new String[] {
            "--databases", "shop", "--store", "jdbc:mysql://db:3306/acta"});
        assertEquals("jdbc:mysql://db:3306/acta", options.storeUrl("shop"));
    }

    @Test
    void severalDatabasesCannotShareOneSqlDatabase() {
        assertThrows(IllegalArgumentException.class, () -> CoShimNode.Options.parse(new String[] {
            "--databases", "shop,billing", "--store", "jdbc:mysql://db:3306/acta"}));
    }

    @Test
    void unsupportedStoresAreRejected() {
        assertThrows(IllegalArgumentException.class, () -> CoShimNode.Options.parse(new String[] {
            "--store", "jdbc:h2:mem:x"}));
    }
}
