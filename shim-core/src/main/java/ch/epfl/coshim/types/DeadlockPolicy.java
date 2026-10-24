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
  WOUND_WAIT
}
