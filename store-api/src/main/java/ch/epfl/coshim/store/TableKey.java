package ch.epfl.coshim.store;

import java.util.Objects;

/**
 * A key inside one database: a shim manages several tables of its database, so what the protocol
 * calls a key {@code k} is a (table, key) pair. The shim locks and orders on the pair; the
 * {@link KvStore} of a database maps it to its own notion of table (column family, collection,
 * key prefix, ...).
 */
public record TableKey<K>(String table, K key) {

    public TableKey {
        Objects.requireNonNull(table, "table");
        Objects.requireNonNull(key, "key");
    }
}
