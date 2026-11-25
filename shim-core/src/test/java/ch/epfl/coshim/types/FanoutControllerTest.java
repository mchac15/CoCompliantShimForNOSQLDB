package ch.epfl.coshim.types;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.Random;
import org.junit.jupiter.api.Test;

class FanoutControllerTest {

  /** The draw of the first direction (true: up). */
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

  private static final double BIG = 1.5;
  private static final double SMALL = 1.1;

  /** Big steps x 1.5, small x 1.1, eps 2 %, 3 small steps before re-anchoring. */
  private static FanoutController controller(double start, boolean... ups) {
    return new FanoutController(
        new FanoutController.Config(start, 0.25, 8, BIG - 1, SMALL - 1, 0.02, 3, Duration.ofSeconds(1),
            Duration.ZERO),
        new Draws(ups));
  }

  private static void period(FanoutController c, int commits) {
    for (int i = 0; i < commits; i++) {
      c.committed();
    }
    c.step();
  }

  @Test
  void startsWithASmallStepTheDrawnWay() {
    FanoutController c = controller(4, false);
    period(c, 100);                // anchor (4, 100), drawn down
    assertEquals(4 / SMALL, c.current(), 1e-9);
  }

  @Test
  void aFlatPeriodTakesASmallStepOnAgainstTheSameAnchor() {
    FanoutController c = controller(4, false);
    period(c, 100);                // anchor (4, 100): 4 / 1.1
    period(c, 101);                // +1 %: on, small
    period(c, 102);                // +2 % against the anchor, not above eps: on, small
    assertEquals(4 / SMALL / SMALL / SMALL, c.current(), 1e-9);
    period(c, 103);                // +3 % against the anchor (+1 % against the last period): better, big
    assertEquals(4 / SMALL / SMALL / SMALL / BIG, c.current(), 1e-9);
  }

  @Test
  void aClearGainAnchorsThereAndTakesABigStep() {
    FanoutController c = controller(4, true);
    period(c, 100);                // anchor (4, 100): 4 x 1.1
    period(c, 110);                // +10 %: anchor (4.4, 110), big step up
    assertEquals(4 * SMALL * BIG, c.current(), 1e-9);
    period(c, 109);                // -1 % against (4.4, 110): on, small
    assertEquals(4 * SMALL * BIG * SMALL, c.current(), 1e-9);
  }

  @Test
  void aClearDropGoesBackToTheAnchorAndMeasuresItAgain() {
    FanoutController c = controller(4, false);
    period(c, 100);                // anchor (4, 100): 4 / 1.1
    period(c, 99);                 // -1 %: on, small
    period(c, 97);                 // -3 % against the anchor: back to 4, up next
    assertEquals(4, c.current(), 1e-9);
    period(c, 90);                 // the anchor measured again: its rate is now (100 + 90) / 2 = 95
    assertEquals(4 * SMALL, c.current(), 1e-9);
    period(c, 96);                 // +1 % against 95: flat (against 100 it would have been back)
    assertEquals(4 * SMALL * SMALL, c.current(), 1e-9);
  }

  @Test
  void afterMaxFlatSmallStepsItReanchorsAndTurns() {
    FanoutController c = controller(4, true);
    period(c, 100);                // anchor (4, 100): small step 1, up
    period(c, 100);                // small step 2
    period(c, 100);                // small step 3
    assertEquals(4 * SMALL * SMALL * SMALL, c.current(), 1e-9);
    period(c, 100);                // still flat after 3: anchor here, small step down
    assertEquals(4 * SMALL * SMALL, c.current(), 1e-9);
    period(c, 97);                 // -3 % against the new anchor: back to it
    assertEquals(4 * SMALL * SMALL * SMALL, c.current(), 1e-9);
  }

  @Test
  void clampsAtTheBounds() {
    FanoutController c = controller(0.26, false);
    period(c, 100);                // 0.26 / 1.1 < 0.25: clamped
    assertEquals(0.25, c.current(), 1e-9);
    period(c, 100);                // flat, down again: stays
    assertEquals(0.25, c.current(), 1e-9);
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
