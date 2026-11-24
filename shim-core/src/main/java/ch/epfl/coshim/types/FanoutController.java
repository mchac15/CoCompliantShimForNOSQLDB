package ch.epfl.coshim.types;

import java.time.Duration;
import java.util.Locale;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.LongAdder;

/**
 * An adaptive fanout cap: a hill-climb of the fractional cap F on the shim's commit rate. Every
 * {@code period} it compares the commit rate of the period that just ended with that of the one before
 * (each measured after {@code settle}: the txns in flight at a change still ran under the old F):
 * <ul>
 *   <li>a drop by more than {@code eps}: the last step hurt, the direction reverses;
 *   <li>a rise by more than {@code eps}: the last step helped, it keeps the direction;
 *   <li>in between (no clear effect): the next direction is drawn at random, up or down with
 *       probability 1/2.
 * </ul>
 * Then F moves one step, x (1 + step) up or / (1 + step) down, clamped to [min, max] (no turnaround at
 * a bound: a flat step there draws again).
 *
 * <p>Why the random draw: going on in the same direction after a flat step drifted F to a bound by
 * default (with no reversal at all at 50-200 threads it walked from its start down to min and stayed
 * there, 4-14 % below the uncapped shim), and turning up after a flat step pinned F at max under heavy
 * load (where a single step down looks flat but a few of them pay: skew 0.95 / 800 threads, 499 tps
 * against 688). A random draw does not favour either bound.
 *
 * <p>Steps are large on purpose: the throughput moves little with F over a single step (a x 1.4
 * step: 1-5 % per 15 s window under load, about the noise), while a x 2 step showed about 10 %. The
 * default x 1.7 sits between them. Periods are long for the same reason (15 s, 3 s of them settling):
 * with 1 s periods the noise exceeded a step's effect and F random-walked.
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
   * @param step a step's relative size: F x (1 + step) or F / (1 + step)
   * @param eps a change of the commit rate counts only if larger than this share (noise)
   * @param period how often it steps
   * @param settle the commits of a period's first {@code settle} are not counted (they still ran
   *     under the previous F); shorter than the period
   */
  public record Config(
      double start, double min, double max, double step, double eps, Duration period, Duration settle) {
    public static final Config DEFAULT =
        new Config(2, 0.25, 8, 0.7, 0.05, Duration.ofSeconds(15), Duration.ofSeconds(3));

    public Config {
      Objects.requireNonNull(period);
      Objects.requireNonNull(settle);
      if (!(min > 0 && min <= start && start <= max)) {
        throw new IllegalArgumentException("need 0 < min <= start <= max: " + min + ", " + start + ", " + max);
      }
      if (!(step > 0) || !(eps >= 0) || period.isNegative() || period.isZero()) {
        throw new IllegalArgumentException("step must be > 0, eps >= 0, period > 0");
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
  private double lastRate = -1;

  public FanoutController(Config config) {
    this(config, new Random());
  }

  /** @param random the draws of the direction after a flat step (seeded in tests) */
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
    boolean compared = lastRate > 0;
    boolean reversed = compared && rate < lastRate * (1 - config.eps());
    boolean improved = compared && rate > lastRate * (1 + config.eps());
    boolean flat = !reversed && !improved;   // also the first step: nothing to compare yet
    if (reversed) {
      direction = -direction;                // the last step hurt: go back
    } else if (flat) {
      direction = random.nextBoolean() ? 1 : -1;   // no clear effect: up or down at random
    }                                        // improved: keep going
    double next = direction > 0 ? fanout * (1 + config.step()) : fanout / (1 + config.step());
    fanout = Math.max(config.min(), Math.min(config.max(), next));
    lastRate = rate;
    return String.format(Locale.ROOT, "rate=%.1f reversed=%d F=%.3f direction=%d flat=%d", rate,
        reversed ? 1 : 0, fanout, direction, flat ? 1 : 0);
  }

  @Override
  public String toString() {
    return String.format(Locale.ROOT,
        "auto (F %.2f in %.2f..%.2f, step %.2f, eps %.2f, every %d ms, settle %d ms, random direction when flat)",
        config.start(), config.min(), config.max(), config.step(), config.eps(), config.period().toMillis(),
        config.settle().toMillis());
  }
}
