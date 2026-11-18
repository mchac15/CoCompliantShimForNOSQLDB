package ch.epfl.coshim.types;

/**
 * How the execution-phase waits of lock()/upgrade() prevent deadlocks, on top of lock_timeout (which
 * stays as the backstop for cycles across shims). All compare txn ages ({@link Transaction#getTs()},
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
   * Deadlock detection: a txn blocks without condition, and every time it starts waiting behind a new
   * predecessor node it searches the wait-for graph of this shim for a cycle through itself. Only on
   * a cycle is a txn aborted: its youngest member (the same one whoever finds the cycle), wounded if
   * it is another txn, dying if it is the searcher. Every member is blocked in lock()/upgrade() and
   * never executed, so the abort cascades to nobody. A txn that would wait for a younger one without
   * a cycle just waits, where wound-wait would wound it.
   */
  DETECT,
  /**
   * No deadlock prevention: lock()/upgrade() just wait, bounded by lock_timeout. Only for workloads that
   * cannot deadlock, e.g. every txn taking its locks in one global order (MicroBench --randomness 0 with
   * sequential branches): there wound-wait and wait-die abort txns for cycles that never form. With any
   * other workload a deadlock lasts until lock_timeout.
   */
  NONE
}
