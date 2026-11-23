package ch.epfl.coshim.types;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import org.junit.jupiter.api.Test;

class FanoutControllerTest {

  private static FanoutController controller(double start) {
    return new FanoutController(new FanoutController.Config(start, 0.25, 8, 0.25, 0.02, Duration.ofSeconds(1), Duration.ZERO, 2));
  }

  private static void commits(FanoutController c, int n) {
    for (int i = 0; i < n; i++) {
      c.committed();
    }
  }

  @Test
  void keepsItsDirectionWhileTheCommitRateClearlyRises() {
    FanoutController c = controller(4);
    commits(c, 100);
    c.step();                      // first step: down (nothing to compare yet)
    assertEquals(4 / 1.25, c.current(), 1e-9);
    commits(c, 110);
    c.step();                      // +10 %: keep going down
    assertEquals(4 / 1.25 / 1.25, c.current(), 1e-9);
  }

  @Test
  void keepsGoingThroughAFlatStep() {
    // the throughput is about flat in F over a wide range: only going on crosses it
    FanoutController c = controller(4);
    commits(c, 100);
    c.step();                      // 4 -> 3.2
    commits(c, 101);
    c.step();                      // +1 %, within eps: still down, 3.2 -> 2.56
    assertEquals(4 / 1.25 / 1.25, c.current(), 1e-9);
  }

  @Test
  void stopsAtTheTopAndGoesBackAfterAProbeThatDidNotHelp() {
    FanoutController c = controller(6.4);
    commits(c, 100);
    c.step();                      // down, 6.4 -> 5.12
    commits(c, 80);
    c.step();                      // worse: up, back to 6.4
    commits(c, 100);
    c.step();                      // better: up, 6.4 -> 8 (exactly the top)
    commits(c, 100);
    c.step();                      // flat: on up, 10 is past the top: stops at 8, no turnaround
    assertEquals(8, c.current(), 1e-9);
    commits(c, 100);
    c.step();                      // holds (1 of 2)
    assertEquals(8, c.current(), 1e-9);
    commits(c, 100);
    c.step();                      // 2 of 2: probes one step inward
    assertEquals(8 / 1.25, c.current(), 1e-9);
    commits(c, 101);
    c.step();                      // +1 %, not clearly better: back to the top
    assertEquals(8, c.current(), 1e-9);
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
  void leavesTheFloorAfterAProbeThatClearlyHelped() {
    FanoutController c = controller(0.3);
    commits(c, 100);
    c.step();                      // 0.3 / 1.25 < 0.25: stops at 0.25, no turnaround
    assertEquals(0.25, c.current(), 1e-9);
    commits(c, 100);
    c.step();                      // holds
    assertEquals(0.25, c.current(), 1e-9);
    commits(c, 100);
    c.step();                      // probes up
    assertEquals(0.25 * 1.25, c.current(), 1e-9);
    commits(c, 110);
    c.step();                      // +10 %: leaves the floor and goes on up
    assertEquals(0.25 * 1.25 * 1.25, c.current(), 1e-9);
    commits(c, 110);
    c.step();                      // flat: on up, as away from a bound
    assertEquals(0.25 * Math.pow(1.25, 3), c.current(), 1e-9);
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
