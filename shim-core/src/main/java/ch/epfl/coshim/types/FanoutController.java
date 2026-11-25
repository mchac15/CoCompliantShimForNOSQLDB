package ch.epfl.coshim.types;

import java.time.Duration;
import java.util.Locale;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.LongAdder;

/**
 * An adaptive fanout cap: a hill-climb of the fractional cap F on the shim's commit rate, against an
 * anchor. The anchor is the last F that made a clear difference, with the commit rate measured there.
 * Every {@code period} the rate of the period that just ended (measured after {@code settle}: the txns
 * in flight at a change still ran under the old F) is compared with the anchor's:
 * <ul>
 *   <li>higher by more than {@code eps}: this F is better, it becomes the anchor and F takes a big step
 *       on in the same direction (x or / (1 + step));
 *   <li>lower by more than {@code eps}: the drift since the anchor hurt, F jumps back to the anchor and
 *       the direction reverses; the next period measures the anchor again (its rate becomes the mean of
 *       the two readings, so one lucky reading does not keep pulling F back);
 *   <li>in between: no clear effect yet, F takes a small step on (x or / (1 + smallStep)), still
 *       compared with the same anchor; after {@code maxFlat} small steps without a clear effect the
 *       current F becomes the anchor and the direction reverses (F matters little here).
 * </ul>
 * The first direction is drawn at random. F is clamped to [min, max].
 *
 * <p>Why an anchor: one step changes the commit rate by about 1-5 % under load (x 1.4, 15 s windows),
 * within the noise, so a comparison with the previous period alone saw most steps as flat. Against the
 * anchor the small effects of several steps add up until they show.
 *
 * <p>F is fractional: a writer's cap is drawn once, when a txn first asks to depend on it, as floor(F)
 * plus 1 with probability frac(F) (see {@link Transaction#specCap}): x.y dependents on average.
 *
 * <p>One per shim: {@link SpeculationLimits#forShim} copies the configured one, the shim starts it and
 * reports its commits. Its steps go to the node's log, one line each.
 */
public final class FanoutController {

  /**
   * @param start F at start
   * @param min the lowest F, > 0 (0 would be the non-speculative shim)
   * @param max the highest F (where the cap no longer binds)
   * @param step a big step's relative size, after a clear gain: F x (1 + step) or F / (1 + step)
   * @param smallStep a small step's relative size, while there is no clear effect
   * @param eps a change of the commit rate counts only if larger than this share (noise)
   * @param maxFlat small steps without a clear effect before the current F becomes the anchor
   * @param period how often it steps
   * @param settle the commits of a period's first {@code settle} are not counted (they still ran
   *     under the previous F); shorter than the period
   */
  public record Config(
      double start, double min, double max, double step, double smallStep, double eps, int maxFlat,
      Duration period, Duration settle) {
    public static final Config DEFAULT =
        new Config(2, 0.25, 8, 0.4, 0.1, 0.025, 4, Duration.ofSeconds(15), Duration.ofSeconds(3));

    public Config {
      Objects.requireNonNull(period);
      Objects.requireNonNull(settle);
      if (!(min > 0 && min <= start && start <= max)) {
        throw new IllegalArgumentException("need 0 < min <= start <= max: " + min + ", " + start + ", " + max);
      }
      if (!(step > 0) || !(smallStep > 0) || !(eps >= 0) || maxFlat < 1 || period.isNegative()
          || period.isZero()) {
        throw new IllegalArgumentException("need step, smallStep > 0, eps >= 0, maxFlat >= 1, period > 0");
      }
      if (settle.isNegative() || settle.compareTo(period) >= 0) {
        throw new IllegalArgumentException("need 0 <= settle < period: " + settle + ", " + period);
      }
    }
  }

  private final Config config;
  private final Random random;
  private volatile double fanout;
  private final LongAdder commits = new LongAdder();
  // the hill-climb's state, touched by its own thread only
  private int direction;
  private double anchorF;
  private double anchorRate = -1;   // < 0: no period measured yet
  private int smallSteps;           // since the anchor
  private boolean remeasure;        // F is back at the anchor: the next period measures it again

  public FanoutController(Config config) {
    this(config, new Random());
  }

  /** @param random the draw of the first direction (seeded in tests) */
  FanoutController(Config config, Random random) {
    this.config = Objects.requireNonNull(config);
    this.random = Objects.requireNonNull(random);
    this.fanout = config.start();
  }

  /** A fresh controller with the same configuration, not started: one per shim. */
  public FanoutController copy() {
    return new FanoutController(config);
  }

  /** Starts stepping every period, on a daemon thread; each step is logged as a "fanout" line. */
  public void start(String name) {
    Thread t = new Thread(() -> {
      long t0 = System.nanoTime();
      while (true) {
        try {
          Thread.sleep(config.settle().toMillis());
          commits.reset();   // what committed while settling still ran under the previous F
          Thread.sleep(config.period().minus(config.settle()).toMillis());
        } catch (InterruptedException e) {
          return;
        }
        System.out.println("fanout " + name + ": t_ms=" + (System.nanoTime() - t0) / 1_000_000 + " " + step());
      }
    }, "coshim-fanout-" + name);
    t.setDaemon(true);
    t.start();
  }

  /** The shim committed a txn. */
  public void committed() {
    commits.increment();
  }

  /** The current fractional cap. */
  public double current() {
    return fanout;
  }

  /** A writer's cap: floor(F), plus 1 with probability frac(F). */
  int drawCap() {
    double f = fanout;
    int x = (int) Math.floor(f);
    return x + (ThreadLocalRandom.current().nextDouble() < f - x ? 1 : 0);
  }

  /** One hill-climb step; returns its log text. */
  String step() {
    double rate = commits.sumThenReset() / (config.period().minus(config.settle()).toNanos() / 1e9);
    String event;
    if (anchorRate < 0) {                          // first period: the start is the first anchor
      anchor(rate);
      direction = random.nextBoolean() ? 1 : -1;
      moveSmall();
      event = "start";
    } else if (remeasure) {                        // measured the anchor again
      anchorRate = (anchorRate + rate) / 2;
      remeasure = false;
      moveSmall();
      event = "remeasured";
    } else if (rate > anchorRate * (1 + config.eps())) {
      anchor(rate);                                // better: keep going, big
      move(config.step());
      event = "better";
    } else if (rate < anchorRate * (1 - config.eps())) {
      fanout = anchorF;                            // worse: back to the anchor, the other way next
      direction = -direction;
      smallSteps = 0;
      remeasure = true;
      event = "back";
    } else if (smallSteps >= config.maxFlat()) {
      anchor(rate);                                // F matters little here: anchor, the other way
      direction = -direction;
      moveSmall();
      event = "reanchored";
    } else {
      moveSmall();                    // no clear effect yet: on, small
      event = "flat";
    }
    return String.format(Locale.ROOT,
        "rate=%.1f reversed=%d F=%.3f direction=%d flat=%d event=%s anchorF=%.3f anchorRate=%.1f", rate,
        event.equals("back") ? 1 : 0, fanout, direction, event.equals("flat") ? 1 : 0, event, anchorF,
        anchorRate);
  }

  private void anchor(double rate) {
    anchorF = fanout;
    anchorRate = rate;
    smallSteps = 0;
  }

  /** F one step of relative size {@code size} in the current direction, clamped. */
  private void move(double size) {
    double next = direction > 0 ? fanout * (1 + size) : fanout / (1 + size);
    fanout = Math.max(config.min(), Math.min(config.max(), next));
  }

  private void moveSmall() {
    move(config.smallStep());
    smallSteps++;
  }

  @Override
  public String toString() {
    return String.format(Locale.ROOT,
        "auto (F %.2f in %.2f..%.2f, step %.2f, small step %.2f, eps %.3f, max flat %d, every %d ms, "
            + "settle %d ms, anchored)",
        config.start(), config.min(), config.max(), config.step(), config.smallStep(), config.eps(),
        config.maxFlat(), config.period().toMillis(), config.settle().toMillis());
  }
}
