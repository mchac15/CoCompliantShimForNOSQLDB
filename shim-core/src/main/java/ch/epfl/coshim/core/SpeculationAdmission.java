package ch.epfl.coshim.core;

import java.time.Duration;
import java.util.Locale;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.LongAdder;

/**
 * Speculation admission: decides once per txn, at start(), whether it may speculate (be granted
 * locks on executed, not yet committed predecessors) or runs non-speculatively (waits for them to
 * commit, and so can never be a cascade victim, nor carry a cascade to its sibling branches on other
 * shims through a coordinator abort).
 *
 * <p>The signal is shim-wide, because speculation collapses under load as a whole: the dependency
 * chains stay short (depth ~1 at every load) while the share of speculating txns that a cascade
 * aborts climbs with the load (skew 0.95: 3 % at 100 threads, 19 % at the speculative peak, 54 % at
 * 1000). So {@code r} = the share of the branches that speculated (took at least one speculative
 * grant) that ended cascade-aborted, over the recent past: two counters decayed with time constant
 * {@code tau}, plus a prior ({@code priorMean} worth {@code priorWeight} outcomes). Branches that
 * aborted for another cause (wounded, timed out, prepare wait-die, a coordinator abort) are not
 * counted: they abort with or without speculation.
 *
 * <p>Graded, not on/off: a txn speculates with probability {@code p}, {@code pMax} while {@code r <=
 * r* - width}, {@code pMin} once {@code r >= r* + width}, and linear in between; {@code pMin > 0}
 * keeps some txns speculating so that {@code r} keeps measuring what speculating costs now. {@code
 * width = 0} is a hard switch at {@code r*}.
 *
 * <p>The threshold {@code r*} is fixed ({@code rMax}), or with {@code rMax} NaN set by the measured
 * costs: speculating pays while {@code (1 - r) * saved > r * lost}, i.e. {@code r* = saved / (saved +
 * lost)}, with {@code saved} the mean lock wait a speculative commit avoided and {@code lost} the
 * mean lifetime of a cascade-aborted speculating txn (both decayed as r). Each outcome is weighted by
 * its reach, capped at {@code reachCap} (0: no weighting): a speculative commit's saved time by (1 +
 * the txns granted behind it while it was uncommitted, which its early finish let through earlier),
 * a cascaded one's lifetime by (1 + the txns its own abort doomed directly).
 *
 * <p>Ratio mode ({@link Mode#RATIO}) does not judge speculation by its own statistics, which cannot
 * tell where speculating stops paying (the crossover also depends on how well the non-speculative
 * shim would do: at skew 0.99 speculation still wins with 37 % of its txns cascaded, at 0.9 it
 * already loses at 13 %). It runs both modes side by side instead, on random shares of the txns
 * under the same load: {@code p} speculative, at least {@code pMin} and at most {@code pMax} so that
 * both groups always exist, and measures each group's efficiency E = commits / total branch lifetime
 * (committed txns per second spent, aborted lifetimes counted as waste), decayed as r. Every 250 ms
 * {@code p += eta * ln(E_spec / E_nonspec)}, clamped: p drifts toward the mode that does better now,
 * and stays put where both do equally well, the crossover.
 *
 * <p>Statistics only: the outcomes are also kept cumulatively by {@code k}, the number of distinct
 * uncommitted txns the branch depended on (1, 2, 3+), to see whether a per-grant budget on k would
 * separate the risky speculations from the safe ones.
 *
 * <p>Thread-safe: one monitor, taken once per txn start and once per speculating branch's end.
 */
public final class SpeculationAdmission {

  /** How p is set: from the cascade share r against r* (THRESHOLD), or by comparing both modes. */
  public enum Mode {
    THRESHOLD,
    RATIO
  }

  /** Ratio mode: how often p is updated, and the decayed samples each group needs first. */
  private static final long STEP_NANOS = 250_000_000L;
  private static final double MIN_GROUP_SAMPLES = 10;

  /**
   * @param tau time constant of the decay: an outcome {@code tau} old weighs 1/e of a new one
   * @param rMax the fixed threshold r*, in (0, 1); NaN: r* = saved / (saved + lost)
   * @param pMin the share of txns that speculate once r >= r* + width, in [0, pMax]
   * @param pMax the share of txns that speculate while r <= r* - width, in [pMin, 1]
   * @param width half-width of the ramp from pMax to pMin around r*, in [0, 0.5); 0: a hard switch
   * @param reachCap the cap on an outcome's reach (dependents / fan-out); 0: outcomes are not weighted
   * @param priorMean r of a shim without evidence, in [0, 1]
   * @param priorWeight how many outcomes that assumption is worth, > 0
   */
  public record Config(
      Mode mode,
      Duration tau,
      double rMax,
      double pMin,
      double pMax,
      double width,
      int reachCap,
      double eta,
      double priorMean,
      double priorWeight) {
    public static final Config DEFAULT =
        new Config(Mode.THRESHOLD, Duration.ofSeconds(1), Double.NaN, 0.05, 1, 0.1, 10, 0.1, 0.05, 3);

    public Config {
      Objects.requireNonNull(tau);
      if (tau.isNegative() || tau.isZero()) {
        throw new IllegalArgumentException("tau must be positive: " + tau);
      }
      if (!Double.isNaN(rMax) && !(rMax > 0 && rMax < 1)) {
        throw new IllegalArgumentException("rMax must be in (0, 1), or NaN for the measured threshold: " + rMax);
      }
      if (!(pMin >= 0 && pMin <= pMax && pMax <= 1)) {
        throw new IllegalArgumentException("need 0 <= pMin <= pMax <= 1: " + pMin + ", " + pMax);
      }
      if (!(width >= 0 && width < 0.5)) {
        throw new IllegalArgumentException("width must be in [0, 0.5): " + width);
      }
      Objects.requireNonNull(mode);
      if (mode == Mode.RATIO && !(pMin > 0 && pMax < 1)) {
        throw new IllegalArgumentException("ratio mode needs 0 < pMin <= pMax < 1 (both groups measured): "
            + pMin + ", " + pMax);
      }
      if (!(eta > 0)) {
        throw new IllegalArgumentException("eta must be positive: " + eta);
      }
      if (reachCap < 0) {
        throw new IllegalArgumentException("reachCap must be >= 0: " + reachCap);
      }
      if (!(priorMean >= 0 && priorMean <= 1)) {
        throw new IllegalArgumentException("priorMean must be in [0, 1]: " + priorMean);
      }
      if (!(priorWeight > 0)) {
        throw new IllegalArgumentException("priorWeight must be positive: " + priorWeight);
      }
    }

    /** r* from the measured costs rather than fixed. */
    public boolean measuredThreshold() {
      return Double.isNaN(rMax);
    }

    @Override
    public String toString() {
      return String.format(Locale.ROOT,
          mode == Mode.RATIO
              ? "admission[ratio: p in %.2f..%.2f, p += %.3f ln(E_spec / E_nonspec) every 250 ms, tau=%d ms]"
              : "admission[r*=%s, tau=%d ms, p=%.2f..%.2f over r* +- %.3f, reach cap %d, prior=%.3f x %.1f]",
          mode == Mode.RATIO ? new Object[] {pMin, pMax, eta, tau.toMillis()}
              : new Object[] {measuredThreshold() ? "saved/(saved+lost)" : String.format(Locale.ROOT, "%.3f", rMax),
                  tau.toMillis(), pMin, pMax, width, reachCap, priorMean, priorWeight});
    }
  }

  private static final int K_BUCKETS = 3;   // 1, 2, 3+ dependencies

  private final Config config;
  private final double tauNanos;
  // decayed as of lastNanos, under this monitor
  private double outcomes;
  private double cascaded;
  private double savedSum;      // weighted by the reach
  private double savedCount;
  private double lostSum;       // weighted by the reach
  private double lostCount;
  private double dependentsSum; // statistics: the reach of the commits and of the cascades
  private double fanoutSum;
  private long lastNanos = System.nanoTime();
  private double lastP = 1;
  // ratio mode, decayed as the rest: per group, its commits, the total lifetime (s) of its resolved
  // branches, and how many resolved
  private double specCommits;
  private double specLifetimeS;
  private double specResolved;
  private double nonCommits;
  private double nonLifetimeS;
  private double nonResolved;
  private double ratioP;
  private long lastStepNanos = System.nanoTime();
  private final LongAdder steps = new LongAdder();
  // statistics
  private final LongAdder speculative = new LongAdder();
  private final LongAdder nonSpeculative = new LongAdder();
  private final LongAdder forcedGrants = new LongAdder();
  private final LongAdder[] outcomesByK = adders();
  private final LongAdder[] cascadedByK = adders();

  public SpeculationAdmission(Config config) {
    this.config = Objects.requireNonNull(config);
    this.tauNanos = config.tau().toNanos();
    this.ratioP = config.pMax();   // start speculating: the better mode at low load
    this.lastP = config.mode() == Mode.RATIO ? ratioP : 1;
  }

  public Config config() {
    return config;
  }

  /** start(): may this txn speculate? */
  public boolean admit() {
    double p;
    synchronized (this) {
      decayTo(System.nanoTime());
      if (config.mode() == Mode.RATIO) {
        step(System.nanoTime());
        p = ratioP;
      } else {
        p = share();
      }
      lastP = p;
    }
    boolean speculate = p >= 1 || (p > 0 && ThreadLocalRandom.current().nextDouble() < p);
    (speculate ? speculative : nonSpeculative).increment();
    return speculate;
  }

  /**
   * Ratio mode, under this monitor: at most every STEP_NANOS, once both groups have enough recent
   * samples, moves p by eta * ln(E_spec / E_nonspec), clamped to [pMin, pMax].
   */
  private void step(long now) {
    if (now - lastStepNanos < STEP_NANOS) {
      return;
    }
    lastStepNanos = now;
    if (specResolved < MIN_GROUP_SAMPLES || nonResolved < MIN_GROUP_SAMPLES) {
      return;
    }
    double ratio = efficiency(specCommits, specLifetimeS) / efficiency(nonCommits, nonLifetimeS);
    ratioP = Math.max(config.pMin(), Math.min(config.pMax(), ratioP + config.eta() * Math.log(ratio)));
    steps.increment();
  }

  /** Commits per second of branch lifetime; half a commit added so that 0 commits stays finite. */
  private static double efficiency(double commits, double lifetimeS) {
    return (commits + 0.5) / Math.max(lifetimeS, 1e-3);
  }

  /**
   * Any txn of this shim resolved, speculative mode or not, committed or aborted (whatever the
   * cause): its group's efficiency sample for ratio mode. A no-op in threshold mode.
   */
  public void resolved(boolean speculativeMode, boolean committed, long lifetimeNanos) {
    if (config.mode() != Mode.RATIO) {
      return;
    }
    double lifetimeS = lifetimeNanos / 1e9;
    synchronized (this) {
      decayTo(System.nanoTime());
      if (speculativeMode) {
        specResolved += 1;
        specLifetimeS += lifetimeS;
        specCommits += committed ? 1 : 0;
      } else {
        nonResolved += 1;
        nonLifetimeS += lifetimeS;
        nonCommits += committed ? 1 : 0;
      }
    }
  }

  /** The share of txns that speculate now: the ramp from pMax to pMin around r*. */
  private double share() {
    double r = r();
    double threshold = threshold();
    double w = config.width();
    if (w == 0) {
      return r <= threshold ? config.pMax() : config.pMin();
    }
    double x = (r - (threshold - w)) / (2 * w);   // 0 at r* - w, 1 at r* + w
    double clamped = Math.max(0, Math.min(1, x));
    return config.pMax() - (config.pMax() - config.pMin()) * clamped;
  }

  /**
   * A branch that speculated committed: its outcome, the lock wait speculation saved it, and its
   * reach (the txns granted behind it while it was uncommitted).
   */
  public void speculatedCommitted(long savedNanos, int dependencies, int dependents) {
    record(false, savedNanos, dependencies, dependents);
  }

  /**
   * A branch that speculated was cascade-aborted: its outcome, the work lost (its lifetime), and its
   * reach (the txns its own abort doomed directly).
   */
  public void speculatedCascaded(long lifetimeNanos, int dependencies, int fanout) {
    record(true, lifetimeNanos, dependencies, fanout);
  }

  /** A resolved txn's grants that the commit-wait mode had to take speculatively (deadlock guard). */
  public void forcedGrants(int n) {
    if (n > 0) {
      forcedGrants.add(n);
    }
  }

  private void record(boolean cascade, long nanos, int dependencies, int reach) {
    int k = Math.min(Math.max(dependencies, 1), K_BUCKETS) - 1;
    outcomesByK[k].increment();
    if (cascade) {
      cascadedByK[k].increment();
    }
    double weight = 1 + Math.min(Math.max(reach, 0), config.reachCap());
    synchronized (this) {
      decayTo(System.nanoTime());
      outcomes += 1;
      if (cascade) {
        cascaded += 1;
        lostSum += nanos * weight;
        lostCount += 1;
        fanoutSum += reach;
      } else {
        savedSum += nanos * weight;
        savedCount += 1;
        dependentsSum += reach;
      }
    }
  }

  private void decayTo(long now) {
    double w = Math.exp(-(now - lastNanos) / tauNanos);
    outcomes *= w;
    cascaded *= w;
    savedSum *= w;
    savedCount *= w;
    lostSum *= w;
    lostCount *= w;
    dependentsSum *= w;
    fanoutSum *= w;
    specCommits *= w;
    specLifetimeS *= w;
    specResolved *= w;
    nonCommits *= w;
    nonLifetimeS *= w;
    nonResolved *= w;
    lastNanos = now;
  }

  private double r() {
    return (cascaded + config.priorMean() * config.priorWeight()) / (outcomes + config.priorWeight());
  }

  /**
   * r*: fixed, or saved / (saved + lost); 1 (speculate) until both a speculative commit and a
   * cascade have been seen recently, since without cascades there is nothing to weigh.
   */
  private double threshold() {
    if (!config.measuredThreshold()) {
      return config.rMax();
    }
    if (savedCount < 1 || lostCount < 1) {
      return 1;
    }
    double saved = savedSum / savedCount;
    double lost = lostSum / lostCount;
    return saved + lost <= 0 ? 1 : saved / (saved + lost);
  }

  /**
   * One line, key=value, for the node log. p, r and r* as of the last update (not decayed to now):
   * the node logs it at shutdown too, after the load stopped, when they would only show the prior.
   * saved and lost are the reach-weighted means r* uses.
   */
  public synchronized String summary() {
    StringBuilder sb = new StringBuilder(String.format(Locale.ROOT,
        "p=%.3f r=%.4f r_star=%.4f saved_us_mean=%.0f lost_us_mean=%.0f dependents_mean=%.2f fanout_mean=%.2f"
            + " admitted_speculative=%d admitted_nonspeculative=%d forced_grants=%d",
        lastP, r(), threshold(), savedCount > 0 ? savedSum / savedCount / 1000 : 0,
        lostCount > 0 ? lostSum / lostCount / 1000 : 0, savedCount > 0 ? dependentsSum / savedCount : 0,
        lostCount > 0 ? fanoutSum / lostCount : 0, speculative.sum(), nonSpeculative.sum(), forcedGrants.sum()));
    if (config.mode() == Mode.RATIO) {
      double es = efficiency(specCommits, specLifetimeS);
      double en = efficiency(nonCommits, nonLifetimeS);
      sb.append(String.format(Locale.ROOT, " e_spec=%.3f e_nonspec=%.3f e_ratio=%.3f steps=%d", es, en, es / en,
          steps.sum()));
    }
    for (int k = 0; k < K_BUCKETS; k++) {
      sb.append(" k").append(k + 1).append(k + 1 == K_BUCKETS ? "plus" : "").append('=')
          .append(outcomesByK[k].sum()).append('/').append(cascadedByK[k].sum());
    }
    return sb.toString();
  }

  private static LongAdder[] adders() {
    LongAdder[] a = new LongAdder[K_BUCKETS];
    for (int i = 0; i < K_BUCKETS; i++) {
      a[i] = new LongAdder();
    }
    return a;
  }
}
