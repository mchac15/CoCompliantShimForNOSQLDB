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
 * <p>A txn speculates iff speculation is on: off once {@code r > r* + hysteresis}, on again once
 * {@code r < r* - hysteresis}. While it is off, a share {@code probe} of the txns speculates anyway,
 * so that {@code r} keeps measuring what speculating costs now (with no speculating txn it could
 * never come back on). The threshold {@code r*} is fixed ({@code rMax}), or with {@code rMax} NaN
 * set by the measured costs: speculating pays while {@code (1 - r) * saved > r * lost}, i.e. {@code
 * r* = saved / (saved + lost)}, with {@code saved} the mean lock wait a speculative commit avoided
 * and {@code lost} the mean lifetime of a cascade-aborted speculating txn (both decayed as r).
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
   * @param probe share of the txns that speculate while speculation is off, in [0, 1]
   * @param hysteresis half-width of the band around r* in which the decision does not change
   * @param priorMean r of a shim without evidence, in [0, 1]
   * @param priorWeight how many outcomes that assumption is worth, > 0
   */
  public record Config(
      Duration tau, double rMax, double probe, double hysteresis, double priorMean, double priorWeight) {
    public static final Config DEFAULT = new Config(Duration.ofSeconds(1), Double.NaN, 0.05, 0.02, 0.05, 3);

    public Config {
      Objects.requireNonNull(tau);
      if (tau.isNegative() || tau.isZero()) {
        throw new IllegalArgumentException("tau must be positive: " + tau);
      }
      if (!Double.isNaN(rMax) && !(rMax > 0 && rMax < 1)) {
        throw new IllegalArgumentException("rMax must be in (0, 1), or NaN for the measured threshold: " + rMax);
      }
      if (!(probe >= 0 && probe <= 1)) {
        throw new IllegalArgumentException("probe must be in [0, 1]: " + probe);
      }
      if (!(hysteresis >= 0 && hysteresis < 0.5)) {
        throw new IllegalArgumentException("hysteresis must be in [0, 0.5): " + hysteresis);
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
      return String.format(Locale.ROOT, "admission[r*=%s, tau=%d ms, probe=%.2f, hysteresis=%.3f, prior=%.3f x %.1f]",
          measuredThreshold() ? "saved/(saved+lost)" : String.format(Locale.ROOT, "%.3f", rMax),
          tau.toMillis(), probe, hysteresis, priorMean, priorWeight);
    }
  }

  private static final int K_BUCKETS = 3;   // 1, 2, 3+ dependencies

  private final Config config;
  private final double tauNanos;
  // decayed as of lastNanos, under this monitor
  private double outcomes;
  private double cascaded;
  private double savedSum;
  private double savedCount;
  private double lostSum;
  private double lostCount;
  private long lastNanos = System.nanoTime();
  private boolean on = true;
  // statistics
  private final LongAdder speculating = new LongAdder();
  private final LongAdder probes = new LongAdder();
  private final LongAdder nonSpeculative = new LongAdder();
  private final LongAdder forcedGrants = new LongAdder();
  private final LongAdder switches = new LongAdder();
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
  public synchronized boolean admit() {
    decayTo(System.nanoTime());
    double r = r();
    double threshold = threshold();
    boolean was = on;
    if (on && r > threshold + config.hysteresis()) {
      on = false;
    } else if (!on && r < threshold - config.hysteresis()) {
      on = true;
    }
    if (on != was) {
      switches.increment();
    }
    if (on) {
      speculating.increment();
      return true;
    }
    if (config.probe() > 0 && ThreadLocalRandom.current().nextDouble() < config.probe()) {
      probes.increment();
      return true;
    }
    nonSpeculative.increment();
    return false;
  }

  /** A branch that speculated committed: its outcome, and the lock wait speculation saved it. */
  public void speculatedCommitted(long savedNanos, int dependencies) {
    record(false, savedNanos, dependencies);
  }

  /** A branch that speculated was cascade-aborted: its outcome, and the work lost (its lifetime). */
  public void speculatedCascaded(long lifetimeNanos, int dependencies) {
    record(true, lifetimeNanos, dependencies);
  }

  /** A resolved txn's grants that the commit-wait mode had to take speculatively (deadlock guard). */
  public void forcedGrants(int n) {
    if (n > 0) {
      forcedGrants.add(n);
    }
  }

  private void record(boolean cascade, long nanos, int dependencies) {
    int k = Math.min(Math.max(dependencies, 1), K_BUCKETS) - 1;
    outcomesByK[k].increment();
    if (cascade) {
      cascadedByK[k].increment();
    }
    synchronized (this) {
      decayTo(System.nanoTime());
      outcomes += 1;
      if (cascade) {
        cascaded += 1;
        lostSum += nanos;
        lostCount += 1;
      } else {
        savedSum += nanos;
        savedCount += 1;
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

  /** One line, key=value, for the node log. */
  public synchronized String summary() {
    decayTo(System.nanoTime());
    StringBuilder sb = new StringBuilder(String.format(Locale.ROOT,
        "on=%d r=%.4f r_star=%.4f saved_us_mean=%.0f lost_us_mean=%.0f admitted_speculative=%d"
            + " admitted_probe=%d admitted_nonspeculative=%d forced_grants=%d switches=%d",
        on ? 1 : 0, r(), threshold(), savedCount > 0 ? savedSum / savedCount / 1000 : 0,
        lostCount > 0 ? lostSum / lostCount / 1000 : 0, speculating.sum(), probes.sum(), nonSpeculative.sum(),
        forcedGrants.sum(), switches.sum()));
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
