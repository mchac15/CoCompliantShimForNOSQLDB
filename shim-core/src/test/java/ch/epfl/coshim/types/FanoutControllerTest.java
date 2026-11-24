package ch.epfl.coshim.types;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.Random;
import org.junit.jupiter.api.Test;

class FanoutControllerTest {

  /** The direction draws after flat steps, in order (true: up). */
  private static final class Draws extends Random {
    private final boolean[] ups;
    private int next;

    Draws(boolean... ups) {
      this.ups = ups;
    }

    @Override
    public boolean nextBoolean() {
      return ups[next++];
    }
  }

  private static FanoutController controller(double start, boolean... ups) {
    return new FanoutController(
        new FanoutController.Config(start, 0.25, 8, 0.25, 0.02, Duration.ofSeconds(1), Duration.ZERO), new Draws(ups));
  }

  private static void commits(FanoutController c, int n) {
    for (int i = 0; i < n; i++) {
      c.committed();
    }
  }

  @Test
  void aFlatStepGoesTheDrawnWay() {
    FanoutController c = controller(4, false, true);
    commits(c, 100);
    c.step();                      // first step, nothing to compare: drawn down, 4 -> 3.2
    assertEquals(4 / 1.25, c.current(), 1e-9);
    commits(c, 101);
    c.step();                      // +1 %, within eps: drawn up, 3.2 -> 4
    assertEquals(4, c.current(), 1e-9);
  }

  @Test
  void keepsItsDirectionAfterAClearGain() {
    FanoutController c = controller(4, false);
    commits(c, 100);
    c.step();                      // drawn down, 4 -> 3.2
    commits(c, 110);
    c.step();                      // +10 %: on down, no draw
    assertEquals(4 / 1.25 / 1.25, c.current(), 1e-9);
  }

  @Test
  void reversesAfterAClearDrop() {
    FanoutController c = controller(4, false);
    commits(c, 100);
    c.step();                      // drawn down, 4 -> 3.2
    commits(c, 80);
    c.step();                      // -20 %: back up, no draw
    assertEquals(4, c.current(), 1e-9);
  }

  @Test
  void clampsAtABoundWithoutTurningAround() {
    FanoutController c = controller(0.3, false, false, true);
    commits(c, 100);
    c.step();                      // drawn down, 0.3 / 1.25 < 0.25: clamped
    assertEquals(0.25, c.current(), 1e-9);
    commits(c, 100);
    c.step();                      // flat, drawn down again: stays at the bound
    assertEquals(0.25, c.current(), 1e-9);
    commits(c, 100);
    c.step();                      // flat, drawn up: leaves it
    assertEquals(0.25 * 1.25, c.current(), 1e-9);
  }

  @Test
  void aFractionalCapDrawsTheExtraDependentWithItsFraction() {
    FanoutController c = controller(1.25);   // floor 1, + 1 with probability 0.25
    int extra = 0;
    for (int i = 0; i < 20_000; i++) {
      int cap = c.drawCap();
      assertTrue(cap == 1 || cap == 2);
      extra += cap - 1;
    }
    assertEquals(0.25, extra / 20_000.0, 0.02);
  }
}
