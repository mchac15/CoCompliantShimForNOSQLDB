package ch.epfl.coshim.types;

import java.util.Objects;

public record LockType<K>(LockMode mode, K key) {
  public LockType(LockMode mode, K key) {
    this.mode = Objects.requireNonNull(mode);
    this.key = Objects.requireNonNull(key);
  }

  public static enum LockMode {
    EXCLUSIVE,
    SHARED
  }
}
