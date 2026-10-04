package ch.epfl.coshim.types;

import java.util.Objects;

/** An entry of {@code txn.locks_acquired}: the mode held on a key and the node holding it. */
public record LockType<K, V>(LockMode mode, LockNode<K, V> node) {
  public LockType {
    Objects.requireNonNull(mode);
    Objects.requireNonNull(node);
  }

  public static enum LockMode {
    EXCLUSIVE,
    SHARED
  }
}
