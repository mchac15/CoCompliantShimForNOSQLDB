package ch.epfl.coshim.types;

import java.time.Duration;
import java.util.Objects;

/**
 * Per-key cap on speculation: under heavy contention a speculative read sits behind a long queue of
 * uncommitted writers, and one abort among them cascades to the whole tail. Each key keeps, in its
 * {@link Chain}, how often its writers that reached executed ended up aborted (two counters decayed
 * with time constant {@code tau}, so recent outcomes weigh more). lock() then speculates on a key
 * only while the estimated chance that none of the {@code d} uncommitted writers ahead aborts is
 * high enough; otherwise it waits for some of them to commit, as the non-speculative baseline does.
 *
 * <p>The abort probability of a key is {@code (aborts + A0) / (outcomes + N0)} with {@code N0 =
 * priorWeight} and {@code A0 = priorMean * priorWeight}: as if every key had already seen N0
 * outcomes with abort rate priorMean. With little evidence (a cold key, or one whose chain was just
 * emptied and recreated) the estimate is close to priorMean instead of 0% or 100%, and as the real
 * counters decay it drifts back there.
 *
 * @param tau time constant of the decay: an outcome {@code tau} old weighs 1/e of a new one
 * @param theta lock() speculates iff {@code (1 - p)^d >= theta} (0: always, 1: only when d = 0)
 * @param priorMean the assumed abort rate of a key without evidence, in [0, 1]
 * @param priorWeight how many outcomes that assumption is worth, > 0
 */
public record AdaptiveSpeculation(Duration tau, double theta, double priorMean, double priorWeight) {
  public static final AdaptiveSpeculation DEFAULT =
      new AdaptiveSpeculation(Duration.ofMillis(100), 0.5, 0.02, 10);

  public AdaptiveSpeculation {
    Objects.requireNonNull(tau);
    if (tau.isNegative() || tau.isZero()) {
      throw new IllegalArgumentException("tau must be positive: " + tau);
    }
    if (!(theta >= 0 && theta <= 1)) {
      throw new IllegalArgumentException("theta must be in [0, 1]: " + theta);
    }
    if (!(priorMean >= 0 && priorMean <= 1)) {
      throw new IllegalArgumentException("priorMean must be in [0, 1]: " + priorMean);
    }
    if (!(priorWeight > 0)) {
      throw new IllegalArgumentException("priorWeight must be positive: " + priorWeight);
    }
  }

  /** The weight left to an outcome {@code elapsedNanos} old: exp(-elapsed / tau). */
  double decay(long elapsedNanos) {
    return Math.exp(-(double) elapsedNanos / tau.toNanos());
  }

  /**
   * @param aborts the key's decayed abort count, as of now
   * @param outcomes the key's decayed outcome count, as of now
   * @param unresolvedWriters d: the writers ahead whose abort would cascade to the requester
   * @return true iff the requester may speculate on them
   */
  boolean allows(double aborts, double outcomes, int unresolvedWriters) {
    double p = (aborts + priorMean * priorWeight) / (outcomes + priorWeight);
    return Math.pow(1 - p, unresolvedWriters) >= theta;
  }
}
