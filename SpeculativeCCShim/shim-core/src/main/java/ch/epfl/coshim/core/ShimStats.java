package ch.epfl.coshim.core;

import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.atomic.LongAdder;

/**
 * Counters of a shim's commits and aborts, the aborts by cause: what explains a benchmark result
 * (e.g. speculation trading lock waits for cascades). Thread-safe, never reset; a txn counts once,
 * when it actually moves to aborted.
 */
public final class ShimStats {

  public enum AbortCause {
    /** lock()/upgrade() waited lock_timeout (assumed deadlock). */
    LOCK_TIMEOUT,
    /** Lost an upgrade race: read-modify-write against read-modify-write. */
    UPGRADE_CONFLICT,
    /** Doomed by a predecessor's abort (was must_abort), wherever it was finally aborted. */
    CASCADE,
    /** prepare() on a txn that was not executed (end never succeeded). */
    PREPARE_NO,
    /** abort() from the 2PC coordinator on a live txn (e.g. another branch failed). */
    COORDINATOR
  }

  private final LongAdder commits = new LongAdder();
  private final Map<AbortCause, LongAdder> aborts = new EnumMap<>(AbortCause.class);

  ShimStats() {
    for (AbortCause cause : AbortCause.values()) {
      aborts.put(cause, new LongAdder());
    }
  }

  void committed() {
    commits.increment();
  }

  void aborted(AbortCause cause) {
    aborts.get(cause).increment();
  }

  public long commits() {
    return commits.sum();
  }

  public long aborts(AbortCause cause) {
    return aborts.get(cause).sum();
  }

  public long aborts() {
    return aborts.values().stream().mapToLong(LongAdder::sum).sum();
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("commits=").append(commits()).append(" aborts=").append(aborts());
    for (AbortCause cause : AbortCause.values()) {
      sb.append(' ').append(cause.name().toLowerCase()).append('=').append(aborts(cause));
    }
    return sb.toString();
  }
}
