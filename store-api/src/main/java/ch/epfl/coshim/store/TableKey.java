package ch.epfl.coshim.store;

import java.util.Objects;

/**
 * A key inside one database, optionally qualified by a table. Tables are a possibility, not a
 * requirement: many key-value stores have no such abstraction.
 *
 * <ul>
 *   <li>{@code TableKey.of(key)}: a plain key ({@link #table()} is null), for stores without tables.</li>
 *   <li>{@code new TableKey<>(table, key)}: a key in a table, for stores that have them (column
 *       families, collections, ...) or that emulate them (e.g. a key prefix).</li>
 * </ul>
 *
 * The shim locks and orders on the whole pair, so the same key in two tables, or with and without a
 * table, are different keys. What a table means physically is up to the database's {@link KvStore}.
 */
public record TableKey<K>(String table, K key) {

    public TableKey {
        Objects.requireNonNull(key, "key");
    }

    /** A plain key, in no table. */
    public static <K> TableKey<K> of(K key) {
        return new TableKey<>(null, key);
    }

    public boolean hasTable() {
        return table != null;
    }
}
