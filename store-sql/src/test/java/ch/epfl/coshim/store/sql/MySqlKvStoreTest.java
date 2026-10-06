package ch.epfl.coshim.store.sql;

import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

/** Runs when {@code COSHIM_TEST_MYSQL_URL} is set, e.g. to {@code jdbc:mysql://127.0.0.1:3306/acta}. */
@EnabledIfEnvironmentVariable(named = "COSHIM_TEST_MYSQL_URL", matches = "jdbc:mysql:.*")
class MySqlKvStoreTest extends SqlKvStoreContract {

    @Override
    String url() {
        return System.getenv("COSHIM_TEST_MYSQL_URL");
    }
}
