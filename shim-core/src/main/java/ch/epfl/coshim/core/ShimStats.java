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
    /** lock()/upgrade() would have waited for a younger txn (deadlock prevention). */
    DEADLOCK_PREVENTION,
    /** Wound-wait: an older txn wanted a lock this one held while still running. */
    WOUNDED,
    /** Lost an upgrade race: read-modify-write against read-modify-write. */
    UPGRADE_CONFLICT,
    /** Doomed by a predecessor's abort (was must_abort), wherever it was finally aborted. */
    CASCADE,
    /** prepare() on a txn that was not executed (end never succeeded). */
    PREPARE_NO,
    /** abort() from the 2PC coordinator on a live txn (e.g. another branch failed). */
    COORDINATOR,
    /** prepare() would have waited for a younger txn: voted NO (cross-shim deadlock prevention). */
    PREPARE_WAIT_DIE
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
  // cascade attribution: each victim by the cause of the abort at the root of its tree
  private final Map<AbortCause, LongAdder> cascadeVictimsByRoot = new EnumMap<>(AbortCause.class);
  private final Map<AbortCause, LongAdder> cascadeLostUsByRoot = new EnumMap<>(AbortCause.class);
  /** Root aborts (not themselves cascade victims) that doomed at least one txn, and how many in all. */
  private final Map<AbortCause, LongAdder> cascadeTreesByRoot = new EnumMap<>(AbortCause.class);
  private final Map<AbortCause, LongAdder> cascadeTreeVictimsByRoot = new EnumMap<>(AbortCause.class);
  /** Tree sizes (txns doomed transitively by one root abort), in power-of-two buckets. */
  private final LongAdder[] cascadeTreeSize = adders();
  /** Txns marked must_abort by a cascade: all, those already executed; and the blocked ones it spared. */
  private final LongAdder cascadeMarked = new LongAdder();
  private final LongAdder cascadeMarkedExecuted = new LongAdder();
  private final LongAdder cascadeSpared = new LongAdder();
  /** Per depth bucket: lifetime of the aborted txns (µs), and how many of them were cascade victims. */
  private final LongAdder[] lostUsByDepth = adders();
  private final LongAdder[] cascadesByDepth = adders();
  private final LongAdder speculativeGrants = new LongAdder();
  private final LongAdder prepareWaitUs = new LongAdder();
  private final LongAdder prepares = new LongAdder();
  /** Net saving of each commit: max(0, saved - its own prepare wait), in µs. */
  private final LongAdder savedNetUs = new LongAdder();
  /** Time from creation to abort of the txns aborted by a cascade (work speculation lost), in µs. */
  private final LongAdder cascadeLostUs = new LongAdder();
  // the time budget of the branches: where a committed one spent its life, what the aborted ones wasted
  private final LongAdder committedLifetimeUs = new LongAdder();
  private final LongAdder committedLockWaitUs = new LongAdder();
  private final LongAdder committedPrepareWaitUs = new LongAdder();
  private final LongAdder committedPostVoteUs = new LongAdder();
  private final LongAdder abortedLockWaitUs = new LongAdder();
  private final LongAdder abortedPrepareWaitUs = new LongAdder();
  private final Map<AbortCause, LongAdder> lostUs = new EnumMap<>(AbortCause.class);
  /** Speculation limits: lock calls whose speculative grant was refused, by first reason, and their wait. */
  private final LongAdder refusedDepth = new LongAdder();
  private final LongAdder refusedFanout = new LongAdder();
  private final LongAdder refusedWaitUs = new LongAdder();
  /** Running sums of (depth, saved µs) over the commits, for their Pearson correlation. */
  private double n, sx, sy, sxx, syy, sxy;

  ShimStats() {
    for (AbortCause cause : AbortCause.values()) {
      aborts.put(cause, new LongAdder());
      lostUs.put(cause, new LongAdder());
      cascadeVictimsByRoot.put(cause, new LongAdder());
      cascadeLostUsByRoot.put(cause, new LongAdder());
      cascadeTreesByRoot.put(cause, new LongAdder());
      cascadeTreeVictimsByRoot.put(cause, new LongAdder());
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

  /** Power-of-two bucket of v ≥ 1: 1, 2-3, 4-7, ..., the last one "or more". */
  private static int log2Bucket(int v) {
    return Math.min(31 - Integer.numberOfLeadingZeros(v), BUCKETS - 1);
  }

  private static String log2Label(int b) {
    int lo = 1 << b;
    return b == BUCKETS - 1 ? lo + "+" : b == 0 ? "1" : lo + "-" + (2 * lo - 1);
  }

  /**
   * @param depth the txn's max dependency depth at a lock grant
   * @param grants its locks granted on uncommitted predecessors
   * @param savedNanos estimate of the lock waiting that speculation saved it (gross: as if each
   *     speculative grant had waited until its predecessors resolved)
   * @param prepareWaitNanos how long its prepare() waited: speculation moves the wait there instead of
   *     removing it, so the net saving is {@code max(0, savedNanos - prepareWaitNanos)}
   */
  void committed(int depth, int grants, long savedNanos, long prepareWaitNanos) {
    commits.increment();
    long savedUs = TimeUnit.NANOSECONDS.toMicros(savedNanos);
    savedNetUs.add(TimeUnit.NANOSECONDS.toMicros(Math.max(0, savedNanos - prepareWaitNanos)));
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

  /**
   * @param fanout txns this abort marked must_abort directly
   * @param lifetimeNanos time from the txn's creation to this abort: counted as lost to speculation
   *     when the cause is a cascade
   */
  void aborted(AbortCause cause, int depth, int fanout, long lifetimeNanos) {
    aborts.get(cause).increment();
    if (cause == AbortCause.CASCADE) {
      cascadeLostUs.add(TimeUnit.NANOSECONDS.toMicros(lifetimeNanos));
    }
    abortsByDepth[bucket(depth)].increment();
    lostUsByDepth[bucket(depth)].add(TimeUnit.NANOSECONDS.toMicros(lifetimeNanos));
    if (cause == AbortCause.CASCADE) {
      cascadesByDepth[bucket(depth)].increment();
    }
    if (fanout > 0) {
      cascadeFanout[bucket(fanout)].increment();
    }
  }

  /**
   * A cascade victim aborted (once, with {@link #aborted}).
   *
   * @param root the cause of the abort at the root of its cascade tree
   */
  void cascadeVictim(AbortCause root, long lifetimeNanos) {
    cascadeVictimsByRoot.get(root).increment();
    cascadeLostUsByRoot.get(root).add(TimeUnit.NANOSECONDS.toMicros(lifetimeNanos));
  }

  /**
   * One abort's marks.
   *
   * @param marked txns it moved to must_abort
   * @param executed those of them already executed (aborted right away, and cascading in turn)
   * @param spared the txns it met but spared, still blocked in lock() on every key it met them on
   */
  void cascadeMarks(int marked, int executed, int spared) {
    cascadeMarked.add(marked);
    cascadeMarkedExecuted.add(executed);
    cascadeSpared.add(spared);
  }

  /** A root abort (not a cascade victim) doomed {@code victims > 0} txns transitively. */
  void cascadeTree(AbortCause root, int victims) {
    cascadeTreesByRoot.get(root).increment();
    cascadeTreeVictimsByRoot.get(root).add(victims);
    cascadeTreeSize[log2Bucket(victims)].increment();
  }

  /** Cascade victims whose tree started with an abort of that cause. */
  public long cascadeVictimsWithRoot(AbortCause root) {
    return cascadeVictimsByRoot.get(root).sum();
  }

  /** Root aborts of that cause that doomed at least one txn. */
  public long cascadeTreesWithRoot(AbortCause root) {
    return cascadeTreesByRoot.get(root).sum();
  }

  /** Txns doomed transitively by the root aborts of that cause (counted at the root). */
  public long cascadeTreeVictimsWithRoot(AbortCause root) {
    return cascadeTreeVictimsByRoot.get(root).sum();
  }

  /** Txns an abort met behind it but spared: still blocked in lock() on every key it met them on. */
  public long cascadeSpared() {
    return cascadeSpared.sum();
  }

  public long cascadeMarked() {
    return cascadeMarked.sum();
  }

  public long cascadeMarkedExecuted() {
    return cascadeMarkedExecuted.sum();
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

  /** Σ over the commits of max(0, saved - prepare wait), in µs. */
  public long savedNetUs() {
    return savedNetUs.sum();
  }

  /** Σ over the cascade aborts of the time from the txn's creation to its abort, in µs. */
  public long cascadeLostUs() {
    return cascadeLostUs.sum();
  }

  /**
   * The time budget of a committed branch.
   *
   * @param lifetimeNanos creation to the end of commit()
   * @param lockWaitNanos inside lock()/upgrade()
   * @param prepareWaitNanos inside prepare(), waiting for its predecessors to commit
   * @param postVoteNanos from the YES vote to the end of commit(): waiting for the global decision
   */
  void committedBudget(long lifetimeNanos, long lockWaitNanos, long prepareWaitNanos, long postVoteNanos) {
    committedLifetimeUs.add(TimeUnit.NANOSECONDS.toMicros(lifetimeNanos));
    committedLockWaitUs.add(TimeUnit.NANOSECONDS.toMicros(lockWaitNanos));
    committedPrepareWaitUs.add(TimeUnit.NANOSECONDS.toMicros(prepareWaitNanos));
    committedPostVoteUs.add(TimeUnit.NANOSECONDS.toMicros(postVoteNanos));
  }

  /**
   * A resolved txn's lock calls the speculation limits held back (once per txn, at commit or abort).
   *
   * @param waitNanos from each one's first refusal to the end of its wait
   */
  void limited(int byDepth, int byFanout, long waitNanos) {
    refusedDepth.add(byDepth);
    refusedFanout.add(byFanout);
    refusedWaitUs.add(TimeUnit.NANOSECONDS.toMicros(waitNanos));
  }

  /** Lock calls whose speculative grant the depth cap refused at least once. */
  public long refusedByDepth() {
    return refusedDepth.sum();
  }

  /** Lock calls whose speculative grant the fanout cap refused (and the depth cap did not). */
  public long refusedByFanout() {
    return refusedFanout.sum();
  }

  /** The time an aborted branch wasted: creation to abort, by the cause of the abort. */
  void abortedBudget(AbortCause cause, long lifetimeNanos, long lockWaitNanos, long prepareWaitNanos) {
    lostUs.get(cause).add(TimeUnit.NANOSECONDS.toMicros(lifetimeNanos));
    abortedLockWaitUs.add(TimeUnit.NANOSECONDS.toMicros(lockWaitNanos));
    abortedPrepareWaitUs.add(TimeUnit.NANOSECONDS.toMicros(prepareWaitNanos));
  }

  public long committedLifetimeUs() {
    return committedLifetimeUs.sum();
  }

  public long committedLockWaitUs() {
    return committedLockWaitUs.sum();
  }

  public long committedPrepareWaitUs() {
    return committedPrepareWaitUs.sum();
  }

  public long committedPostVoteUs() {
    return committedPostVoteUs.sum();
  }

  public long lostUs(AbortCause cause) {
    return lostUs.get(cause).sum();
  }

  public long abortedLockWaitUs() {
    return abortedLockWaitUs.sum();
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
   * and per depth bucket {@code d:commits/aborts/mean saved µs}, cascade fan-out {@code k:aborts};
   * then the totals (µs) the bench sums over nodes: commits, gross and net saved, lost to cascades,
   * net saved per commit (net saved minus lost, per commit), and the time budget (committed branches:
   * lifetime, lock wait, prepare wait, post-vote wait; aborted ones: lock and prepare wait, lifetime
   * by cause), and the speculation limits' refusals (lock calls held back by depth / fanout, and the
   * time they waited because of it).
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
    long net = savedNetUs.sum() - cascadeLostUs.sum();
    sb.append(String.format(Locale.ROOT,
        "commits=%d saved_us_total=%d saved_net_us_total=%d cascade_lost_us_total=%d net_saved_us_per_commit=%.0f",
        c, saved, savedNetUs.sum(), cascadeLostUs.sum(), c == 0 ? 0 : (double) net / c));
    sb.append(String.format(Locale.ROOT,
        " committed_lifetime_us_total=%d committed_lock_wait_us_total=%d committed_prepare_wait_us_total=%d"
            + " committed_post_vote_us_total=%d aborted_lock_wait_us_total=%d aborted_prepare_wait_us_total=%d",
        committedLifetimeUs.sum(), committedLockWaitUs.sum(), committedPrepareWaitUs.sum(), committedPostVoteUs.sum(),
        abortedLockWaitUs.sum(), abortedPrepareWaitUs.sum()));
    sb.append(String.format(Locale.ROOT,
        " spec_refused_depth_total=%d spec_refused_fanout_total=%d spec_refused_wait_us_total=%d",
        refusedDepth.sum(), refusedFanout.sum(), refusedWaitUs.sum()));
    for (AbortCause cause : AbortCause.values()) {
      sb.append(" lost_").append(cause.name().toLowerCase()).append("_us_total=").append(lostUs(cause));
    }
    sb.append(' ').append(cascadeSummary());
    return sb.toString().trim();
  }

  /**
   * The cascade attribution, appended to {@link #speculationSummary}: per depth bucket {@code
   * d:aborts/cascade aborts/mean lifetime µs of the aborts}, tree sizes {@code size range:roots}, then
   * totals (named *_total, never lost_*_us_total, which the bench sums as lost time): the marks (all,
   * executed) and the blocked txns spared, and per root cause the victims, their lost µs, the trees and the
   * victims counted at their roots (the two victim counts differ by the victims of trees whose
   * executed txns another thread aborted first, e.g. the coordinator).
   */
  String cascadeSummary() {
    StringBuilder sb = new StringBuilder("depth_lost=");
    for (int d = 0; d < BUCKETS; d++) {
      long da = abortsByDepth[d].sum();
      if (da > 0) {
        sb.append(d).append(d == BUCKETS - 1 ? "+" : "").append(':').append(da).append('/')
            .append(cascadesByDepth[d].sum()).append('/').append(lostUsByDepth[d].sum() / da).append(' ');
      }
    }
    sb.append("cascade_tree=");
    for (int b = 0; b < BUCKETS; b++) {
      long f = cascadeTreeSize[b].sum();
      if (f > 0) {
        sb.append(log2Label(b)).append(':').append(f).append(' ');
      }
    }
    sb.append(String.format(Locale.ROOT,
        "cascade_marked_total=%d cascade_marked_executed_total=%d cascade_spared_total=%d",
        cascadeMarked.sum(), cascadeMarkedExecuted.sum(), cascadeSpared.sum()));
    for (AbortCause root : AbortCause.values()) {
      if (root == AbortCause.CASCADE) {
        continue;   // never a root
      }
      String r = root.name().toLowerCase();
      sb.append(" cascade_root_").append(r).append("_victims_total=").append(cascadeVictimsByRoot.get(root).sum())
          .append(" cascade_root_").append(r).append("_victim_us_total=").append(cascadeLostUsByRoot.get(root).sum())
          .append(" cascade_root_").append(r).append("_trees_total=").append(cascadeTreesByRoot.get(root).sum())
          .append(" cascade_root_").append(r).append("_tree_victims_total=")
          .append(cascadeTreeVictimsByRoot.get(root).sum());
    }
    return sb.toString();
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
