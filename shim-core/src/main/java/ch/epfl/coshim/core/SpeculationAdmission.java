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
 * <p>Statistics only: the outcomes are also kept cumulatively by {@code k}, the number of distinct
 * uncommitted txns the branch depended on (1, 2, 3+), to see whether a per-grant budget on k would
 * separate the risky speculations from the safe ones.
 *
 * <p>Thread-safe: one monitor, taken once per txn start and once per speculating branch's end.
 */
public final class SpeculationAdmission {

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
      Duration tau,
      double rMax,
      double pMin,
      double pMax,
      double width,
      int reachCap,
      double priorMean,
      double priorWeight) {
    public static final Config DEFAULT =
        new Config(Duration.ofSeconds(1), Double.NaN, 0.05, 1, 0.1, 10, 0.05, 3);

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
          "admission[r*=%s, tau=%d ms, p=%.2f..%.2f over r* +- %.3f, reach cap %d, prior=%.3f x %.1f]",
          measuredThreshold() ? "saved/(saved+lost)" : String.format(Locale.ROOT, "%.3f", rMax),
          tau.toMillis(), pMin, pMax, width, reachCap, priorMean, priorWeight);
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
  // statistics
  private final LongAdder speculative = new LongAdder();
  private final LongAdder nonSpeculative = new LongAdder();
  private final LongAdder forcedGrants = new LongAdder();
  private final LongAdder[] outcomesByK = adders();
  private final LongAdder[] cascadedByK = adders();

  public SpeculationAdmission(Config config) {
    this.config = Objects.requireNonNull(config);
    this.tauNanos = config.tau().toNanos();
  }

  public Config config() {
    return config;
  }

  /** start(): may this txn speculate? */
  public boolean admit() {
    double p;
    synchronized (this) {
      decayTo(System.nanoTime());
      p = share();
      lastP = p;
    }
    boolean speculate = p >= 1 || (p > 0 && ThreadLocalRandom.current().nextDouble() < p);
    (speculate ? speculative : nonSpeculative).increment();
    return speculate;
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
