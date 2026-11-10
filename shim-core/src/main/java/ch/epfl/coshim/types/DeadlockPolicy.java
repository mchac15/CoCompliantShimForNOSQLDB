package ch.epfl.coshim.types;

/**
 * How the execution-phase waits of lock()/upgrade() prevent deadlocks, on top of lock_timeout (which
 * stays as the backstop for cycles across shims). Both compare txn ages ({@link Transaction#getTs()},
 * smaller = older); prepare()'s waits are never affected.
 */
public enum DeadlockPolicy {
  /** A txn that would wait for a younger one dies (aborts itself) instead. */
  WAIT_DIE,
  /**
   * A txn that would wait for a younger running (started) one wounds it: the younger is aborted and
   * its locks released, and the older waits for its node to empty. A younger txn waits for an older.
   */
  WOUND_WAIT,
  /**
   * Wound-wait that wounds a younger running holder only while it is blocked itself (waiting in
   * lock()/upgrade()); while it runs, the older txn waits for it. A deadlock is a cycle of blocked txns, and its
   * oldest member waits for a younger one: when that one blocks, the txns waiting behind it re-check (it wakes
   * the chains it holds) and the oldest wounds it. So no cycle survives, and a younger txn that would just have
   * finished executing is not aborted for nothing.
   */
  WOUND_BLOCKED,
  /**
   * No deadlock prevention: lock()/upgrade() just wait, bounded by lock_timeout. Only for workloads that
   * cannot deadlock, e.g. every txn taking its locks in one global order (MicroBench --randomness 0 with
   * sequential branches): there wound-wait and wait-die abort txns for cycles that never form. With any
   * other workload a deadlock lasts until lock_timeout.
   */
  NONE;

  /** The policies that wound younger holders instead of waiting for them. */
  public boolean wounds() {
    return this == WOUND_WAIT || this == WOUND_BLOCKED;
  }
}
