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
   * No deadlock prevention: lock()/upgrade() just wait, bounded by lock_timeout. Only for workloads that
   * cannot deadlock, e.g. every txn taking its locks in one global order: there wound-wait and
   * wait-die abort txns for cycles that never form. With any other workload a deadlock lasts until
   * lock_timeout.
   */
  NONE
}
