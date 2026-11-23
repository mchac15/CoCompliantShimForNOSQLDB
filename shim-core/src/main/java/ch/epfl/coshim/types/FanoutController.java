package ch.epfl.coshim.types;

import java.time.Duration;
import java.util.Locale;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.LongAdder;

/**
 * An adaptive fanout cap: a hill-climb of the fractional cap F on the shim's commit rate. Every
 * {@code period} it compares the commit rate of the period that just ended with that of the one before
 * (each measured after {@code settle}: the txns in flight at a change still ran under the old F):
 * if they fell by more than {@code eps}, the last step hurt and the direction reverses; otherwise it
 * keeps going, also when nothing changed. Then F moves one step, x (1 + step) up or / (1 + step) down,
 * within [min, max].
 *
 * <p>A bound is sticky: F stops there, with no turnaround (a turnaround there is a reversal no
 * measurement asked for: with steps that rarely clear the noise, F swept from bound to bound). Every
 * {@code probeEvery} periods it probes one step inward, and keeps it only if the commit rate clearly
 * rose (by more than {@code eps}); then it goes on inward, else back to the bound. Under heavy load the
 * best caps are near min (0.3-0.5 at 600-1000 threads, skew 0.95), where F now stays. The best static cap falls with the load (skew 0.99:
 * 4 or more at 100 threads, 1 at 1000), so F should follow it.
 *
 * <p>Going on through a flat stretch matters: the throughput is about flat in F from 8 down to about
 * 2, and only a cap near 1 or below pays under heavy load. A variant that stepped up on a flat step
 * (to stay high at low load) never crossed that stretch: at skew 0.95 / 800 threads F stayed near 8
 * and the throughput fell to about the uncapped one (499 tps against 688). The price of going on: at
 * low load, where the cap never binds, F sweeps between the bounds (4-6 % lost at 100-200 threads).
 *
 * <p>The defaults are slow on purpose: with 1 s periods, +-15 % steps and a 2 % margin, the noise of a
 * second's commit count (about +-6 % on the difference of two windows) exceeded the effect of a step
 * (3-4 %), F random-walked (25-50 % of the steps reversed) and ended about the reverse of the best
 * static caps (skew 0.95). Now 5 s periods (1 s of them settling), x 1.4 steps and a 5 % margin.
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
   * @param eps a drop of the commit rate counts only if larger than this share (noise)
   * @param period how often it steps
   * @param settle the commits of a period's first {@code settle} are not counted (they still ran
   *     under the previous F); shorter than the period
   * @param probeEvery at a bound, how many periods F stays before probing one step inward, >= 1
   */
  public record Config(
      double start, double min, double max, double step, double eps, Duration period, Duration settle,
      int probeEvery) {
    public static final Config DEFAULT =
        new Config(4, 0.25, 8, 0.4, 0.05, Duration.ofSeconds(5), Duration.ofSeconds(1), 4);

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
      if (probeEvery < 1) {
        throw new IllegalArgumentException("probeEvery must be >= 1: " + probeEvery);
      }
    }
  }

  private final Config config;
  private volatile double fanout;
  private final LongAdder commits = new LongAdder();
  // the hill-climb's state, touched by its own thread only
  private int direction = -1;   // start by trying a lower cap
  private double lastRate = -1;
  private boolean atBound;      // F stopped at min or max
  private boolean probing;      // F is one step inward of the bound, on trial
  private int held;             // periods spent at the bound since the last probe

  public FanoutController(Config config) {
    this.config = Objects.requireNonNull(config);
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
    boolean reversed = false;
    boolean flat = false;
    String bound = "no";
    if (atBound) {
      double edge = fanout >= config.max() || probing && direction < 0 ? config.max() : config.min();
      int inward = edge == config.max() ? -1 : 1;
      if (probing) {
        probing = false;
        if (compared && rate > lastRate * (1 + config.eps())) {
          atBound = false;   // the step inward clearly helped: leave the bound, go on inward
          direction = inward;
          bound = "left";
        } else {
          fanout = edge;     // it did not: back to the bound
          held = 0;
          lastRate = rate;
          return log(rate, false, false, "back");
        }
      } else if (++held >= config.probeEvery()) {
        held = 0;
        probing = true;
        direction = inward;
        fanout = inward > 0 ? fanout * (1 + config.step()) : fanout / (1 + config.step());
        lastRate = rate;     // the probe is judged against the bound's rate
        return log(rate, false, false, "probe");
      } else {
        lastRate = rate;
        return log(rate, false, false, "hold");
      }
    } else {
      reversed = compared && rate < lastRate * (1 - config.eps());
      flat = compared && !reversed && rate <= lastRate * (1 + config.eps());
      if (reversed) {
        direction = -direction;   // the last step made it worse: go back
      }                           // else keep going, also through a flat stretch (see the class comment)
    }
    double next = direction > 0 ? fanout * (1 + config.step()) : fanout / (1 + config.step());
    if (next > config.max() || next < config.min()) {
      next = Math.max(config.min(), Math.min(config.max(), next));
      atBound = true;             // stop there, no turnaround: see the class comment
      held = 0;
    }
    fanout = next;
    lastRate = rate;
    return log(rate, reversed, flat, bound);
  }

  /** A step's log text (the fields before bound= keep the format of the earlier versions). */
  private String log(double rate, boolean reversed, boolean flat, String bound) {
    return String.format(Locale.ROOT, "rate=%.1f reversed=%d F=%.3f direction=%d flat=%d bound=%s", rate,
        reversed ? 1 : 0, fanout, direction, flat ? 1 : 0, bound);
  }

  @Override
  public String toString() {
    return String.format(Locale.ROOT,
        "auto (F %.2f in %.2f..%.2f, step %.2f, eps %.2f, every %d ms, settle %d ms, sticky bounds probed every %d)",
        config.start(), config.min(), config.max(), config.step(), config.eps(), config.period().toMillis(),
        config.settle().toMillis(), config.probeEvery());
  }
}
