package ch.epfl.coshim.types;

/**
 * Caps on speculation, checked by lock()/upgrade() at the speculation point (every predecessor holder
 * executed): a grant that would exceed one is refused, and the txn waits like the non-speculative
 * baseline until the txns ahead commit enough for it to fit (see {@link Chain#awaitLockReady}).
 *
 * <p>Both count the txns whose abort would doom the grantee on that key: the undecided (not
 * committing / committed) holders of the EXCLUSIVE nodes ahead of its node, back to the first blind
 * writer, as abort_transaction's cascade marks them. A blind write depends on nobody.
 *
 * @param maxDepth at most that many such txns ahead of a grant on one key; 0 disables speculation
 * @param maxFanout at most that many running txns depending on one txn (a dependent counts once,
 *     whatever the number of keys it depends on it through); 0 disables speculation
 * @param fanout an adaptive fanout cap instead of maxFanout (each writer's cap drawn from its current
 *     fractional value, see {@link FanoutController}); null: maxFanout
 */
public record SpeculationLimits(int maxDepth, int maxFanout, FanoutController fanout) {
  public static final int NO_LIMIT = Integer.MAX_VALUE;
  /** pseudo.txt: every grant at the speculation point. */
  public static final SpeculationLimits UNLIMITED = new SpeculationLimits(NO_LIMIT, NO_LIMIT);

  public SpeculationLimits {
    if (maxDepth < 0 || maxFanout < 0) {
      throw new IllegalArgumentException("speculation limits are non-negative");
    }
  }

  public SpeculationLimits(int maxDepth, int maxFanout) {
    this(maxDepth, maxFanout, null);
  }

  /** The limits one shim uses: with an adaptive fanout cap, a fresh controller of its own. */
  public SpeculationLimits forShim() {
    return fanout == null ? this : new SpeculationLimits(maxDepth, maxFanout, fanout.copy());
  }

  public boolean isUnlimited() {
    return maxDepth == NO_LIMIT && maxFanout == NO_LIMIT && fanout == null;
  }

  /** Neither cap is 0: some speculation is allowed (0 makes it the non-speculative baseline). */
  boolean allowsSpeculation() {
    return maxDepth > 0 && maxFanout > 0;
  }

  boolean limitsFanout() {
    return maxFanout != NO_LIMIT || fanout != null;
  }

  /** The cap of writer {@code t}: drawn once from the adaptive cap, or maxFanout. */
  int fanoutCap(Transaction<?, ?> t) {
    return fanout == null ? maxFanout : t.specCap(fanout);
  }

  @Override
  public String toString() {
    return "depth " + (maxDepth == NO_LIMIT ? "none" : maxDepth)
        + ", fanout " + (fanout != null ? fanout : maxFanout == NO_LIMIT ? "none" : maxFanout);
  }
}
