package ch.epfl.coshim.store.sql;

import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

/** Runs when {@code COSHIM_TEST_PG_URL} is set, e.g. to {@code jdbc:postgresql://127.0.0.1:5432/acta}. */
@EnabledIfEnvironmentVariable(named = "COSHIM_TEST_PG_URL", matches = "jdbc:postgresql:.*")
class PostgresKvStoreTest extends SqlKvStoreContract {

    @Override
    String url() {
        return System.getenv("COSHIM_TEST_PG_URL");
    }
}
