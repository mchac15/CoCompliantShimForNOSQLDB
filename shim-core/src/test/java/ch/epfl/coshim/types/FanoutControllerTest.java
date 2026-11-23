package ch.epfl.coshim.types;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import org.junit.jupiter.api.Test;

class FanoutControllerTest {

  private static FanoutController controller(double start) {
    return new FanoutController(new FanoutController.Config(start, 0.25, 8, 0.25, 0.02, Duration.ofSeconds(1)));
  }

  private static void commits(FanoutController c, int n) {
    for (int i = 0; i < n; i++) {
      c.committed();
    }
  }

  @Test
  void keepsItsDirectionWhileTheCommitRateDoesNotDrop() {
    FanoutController c = controller(4);
    commits(c, 100);
    c.step();                      // first step: down (nothing to compare yet)
    assertEquals(4 / 1.25, c.current(), 1e-9);
    commits(c, 110);
    c.step();                      // better: keep going down
    assertEquals(4 / 1.25 / 1.25, c.current(), 1e-9);
    commits(c, 109);
    c.step();                      // within eps: still down
    assertEquals(4 / Math.pow(1.25, 3), c.current(), 1e-9);
  }

  @Test
  void reversesWhenTheLastStepHurt() {
    FanoutController c = controller(4);
    commits(c, 100);
    c.step();                      // 4 -> 3.2
    commits(c, 80);
    c.step();                      // -20 %: back up, 3.2 -> 4
    assertEquals(4, c.current(), 1e-9);
  }

  @Test
  void turnsAroundAtABound() {
    FanoutController c = controller(0.3);
    commits(c, 100);
    c.step();                      // 0.3 / 1.25 < 0.25: clamped, turns around
    assertEquals(0.25, c.current(), 1e-9);
    commits(c, 100);
    c.step();
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
