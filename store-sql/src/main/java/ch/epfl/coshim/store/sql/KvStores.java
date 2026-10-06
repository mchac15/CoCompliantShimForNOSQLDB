package ch.epfl.coshim.store.sql;

import ch.epfl.coshim.store.InMemoryKvStore;
import ch.epfl.coshim.store.KvStore;
import ch.epfl.coshim.store.TableKey;

/** Picks a shim's {@link KvStore} from a URL. */
public final class KvStores {

    private KvStores() {}

    /**
     * {@code memory}: an {@link InMemoryKvStore}; {@code jdbc:coshim:mysql://...} or
     * {@code jdbc:coshim:pg://...}: a {@link SqlKvStore} filled with {@code entries} keys.
     */
    public static KvStore<TableKey<String>, String> fromUrl(String url, int entries) {
        if (url.equals("memory")) {
            return new InMemoryKvStore<>();
        }
        if (url.startsWith(SqlKvStore.PREFIX)) {
            return new SqlKvStore(url, entries);
        }
        throw new IllegalArgumentException("unknown store (memory, jdbc:coshim:mysql://..., jdbc:coshim:pg://...): " + url);
    }
}
