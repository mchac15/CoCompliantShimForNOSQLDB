package ch.epfl.coshim.core;

import java.util.Arrays;
import java.util.EnumMap;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;
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

  /**
   * Buckets of the speculation histograms: a txn's depth is the most nodes with uncommitted holders
   * ahead of it at any of its lock grants (0: it never speculated), the last bucket is "or more".
   * Same buckets for the cascade fan-out (txns one abort marked must_abort).
   */
  public static final int BUCKETS = 11;

  private final LongAdder commits = new LongAdder();
  private final Map<AbortCause, LongAdder> aborts = new EnumMap<>(AbortCause.class);
  private final LongAdder[] commitsByDepth = adders();
  private final LongAdder[] abortsByDepth = adders();
  private final LongAdder[] savedUsByDepth = adders();
  private final LongAdder[] cascadeFanout = adders();
  private final LongAdder speculativeGrants = new LongAdder();
  private final LongAdder prepareWaitUs = new LongAdder();
  private final LongAdder prepares = new LongAdder();
  /** Running sums of (depth, saved µs) over the commits, for their Pearson correlation. */
  private double n, sx, sy, sxx, syy, sxy;

  ShimStats() {
    for (AbortCause cause : AbortCause.values()) {
      aborts.put(cause, new LongAdder());
    }
  }

  private static LongAdder[] adders() {
    LongAdder[] a = new LongAdder[BUCKETS];
    Arrays.setAll(a, i -> new LongAdder());
    return a;
  }

  private static int bucket(int v) {
    return Math.min(v, BUCKETS - 1);
  }

  /**
   * @param depth the txn's max dependency depth at a lock grant
   * @param grants its locks granted on uncommitted predecessors
   * @param savedNanos estimate of the lock waiting that speculation saved it
   */
  void committed(int depth, int grants, long savedNanos) {
    commits.increment();
    long savedUs = TimeUnit.NANOSECONDS.toMicros(savedNanos);
    commitsByDepth[bucket(depth)].increment();
    savedUsByDepth[bucket(depth)].add(savedUs);
    speculativeGrants.add(grants);
    synchronized (this) {
      n++;
      sx += depth;
      sy += savedUs;
      sxx += (double) depth * depth;
      syy += (double) savedUs * savedUs;
      sxy += (double) depth * savedUs;
    }
  }

  /** @param fanout txns this abort marked must_abort directly */
  void aborted(AbortCause cause, int depth, int fanout) {
    aborts.get(cause).increment();
    abortsByDepth[bucket(depth)].increment();
    if (fanout > 0) {
      cascadeFanout[bucket(fanout)].increment();
    }
  }

  /** prepare() waited this long for its predecessors to commit (the cost of ordering). */
  void prepared(long waitNanos) {
    prepares.increment();
    prepareWaitUs.add(TimeUnit.NANOSECONDS.toMicros(waitNanos));
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

  /** Commits whose max dependency depth falls in that bucket. */
  public long commitsAtDepth(int depth) {
    return commitsByDepth[bucket(depth)].sum();
  }

  /** Estimated lock waiting speculation saved the commits of that depth bucket, in µs. */
  public long savedUsAtDepth(int depth) {
    return savedUsByDepth[bucket(depth)].sum();
  }

  /** Aborts that marked that many txns must_abort directly. */
  public long abortsWithFanout(int fanout) {
    return cascadeFanout[bucket(fanout)].sum();
  }

  /** Pearson correlation of depth and saved time over the commits; NaN if undefined. */
  public synchronized double depthSavedCorrelation() {
    double cov = n * sxy - sx * sy;
    double var = (n * sxx - sx * sx) * (n * syy - sy * sy);
    return var <= 0 ? Double.NaN : cov / Math.sqrt(var);
  }

  /**
   * One line on what speculation did: share of commits that speculated, saved time, prepare waits,
   * and per depth bucket {@code d:commits/aborts/mean saved µs}, cascade fan-out {@code k:aborts}.
   */
  public String speculationSummary() {
    long c = commits();
    long speculated = c - commitsByDepth[0].sum();
    long saved = Arrays.stream(savedUsByDepth).mapToLong(LongAdder::sum).sum();
    StringBuilder sb = new StringBuilder(String.format(Locale.ROOT,
        "speculated_commits=%d (%.1f%%) spec_grants_per_commit=%.2f saved_us_per_commit=%.0f"
            + " corr_depth_saved=%.3f prepare_wait_us_mean=%.0f",
        speculated, c == 0 ? 0 : 100.0 * speculated / c, c == 0 ? 0 : (double) speculativeGrants.sum() / c,
        c == 0 ? 0 : (double) saved / c, depthSavedCorrelation(),
        prepares.sum() == 0 ? 0 : (double) prepareWaitUs.sum() / prepares.sum()));
    sb.append(" depth=");
    for (int d = 0; d < BUCKETS; d++) {
      long dc = commitsByDepth[d].sum();
      long da = abortsByDepth[d].sum();
      if (dc + da > 0) {
        sb.append(d).append(d == BUCKETS - 1 ? "+" : "").append(':').append(dc).append('/').append(da)
            .append('/').append(dc == 0 ? 0 : savedUsByDepth[d].sum() / dc).append(' ');
      }
    }
    sb.append("cascade_fanout=");
    for (int k = 1; k < BUCKETS; k++) {
      long f = cascadeFanout[k].sum();
      if (f > 0) {
        sb.append(k).append(k == BUCKETS - 1 ? "+" : "").append(':').append(f).append(' ');
      }
    }
    return sb.toString().trim();
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
