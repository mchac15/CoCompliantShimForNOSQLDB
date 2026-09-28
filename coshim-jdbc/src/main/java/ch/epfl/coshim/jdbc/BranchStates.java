package ch.epfl.coshim.jdbc;

import java.time.Duration;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * XA-layer bookkeeping that the shim protocol deliberately does not keep (pseudo.txt: the
 * coordinator never sends anything for a txn before its execute, and there are no tombstones).
 * Seata does not guarantee that ordering: a TC rollback (e.g. global timeout) can reach the RM after
 * {@code branchRegister} but before {@code xa start}, or while phase 1 is still running. The shim's
 * abort is then a no-op or comes too early, and the branch would go on, vote YES and keep its locks
 * forever. Per branch:
 *
 * <ul>
 *   <li>{@code ACTIVE}: between start and a successful prepare (phase 1 running);</li>
 *   <li>{@code PREPARED}: voted YES, waiting for phase 2;</li>
 *   <li>{@code ROLLED_BACK}: the TC rolled back while the branch was ACTIVE, or before it started
 *       (a tombstone). The phase-1 thread aborts the txn when it sees it; a later start is refused.</li>
 * </ul>
 *
 * Entries are removed on commit, local rollback and when the phase-1 thread consumes a
 * ROLLED_BACK mark. Tombstones for branches that never start again (e.g. a retried TC rollback of a
 * finished branch) expire after {@link #TOMBSTONE_TTL}.
 */
final class BranchStates {

    static final Duration TOMBSTONE_TTL = Duration.ofMinutes(10);

    private enum State { ACTIVE, PREPARED, ROLLED_BACK }

    private record Entry(State state, long sinceNanos) {}

    private final ConcurrentMap<String, Entry> states = new ConcurrentHashMap<>();

    /** Before {@code shim.start}. @return false if the TC already rolled the branch back. */
    boolean activate(String txnId) {
        Entry e = states.compute(txnId, (k, cur) -> cur == null ? new Entry(State.ACTIVE, System.nanoTime()) : cur);
        if (e.state == State.ROLLED_BACK) {
            states.remove(txnId);
            return false;
        }
        return true;
    }

    /** After each phase-1 step: did the TC roll the branch back meanwhile? */
    boolean isRolledBack(String txnId) {
        Entry e = states.get(txnId);
        return e != null && e.state == State.ROLLED_BACK;
    }

    /** After a YES vote. */
    void prepared(String txnId) {
        states.computeIfPresent(txnId, (k, cur) -> cur.state == State.ACTIVE ? new Entry(State.PREPARED, cur.sinceNanos) : cur);
    }

    /** The branch is over (commit, local rollback, failed phase 1 observed by its own thread). */
    void finished(String txnId) {
        states.remove(txnId);
    }

    /** A rollback coming from the TC (phase 2, or racing phase 1). */
    void rolledBackByCoordinator(String txnId) {
        long now = System.nanoTime();
        states.compute(txnId, (k, cur) -> cur != null && cur.state == State.PREPARED
                ? null                                    // normal phase 2: the abort that follows is enough
                : new Entry(State.ROLLED_BACK, now));     // phase 1 running or not started yet
        pruneTombstones(now);
    }

    int size() {
        return states.size();
    }

    private void pruneTombstones(long now) {
        long ttl = TOMBSTONE_TTL.toNanos();
        states.entrySet().removeIf(en -> en.getValue().state == State.ROLLED_BACK && now - en.getValue().sinceNanos > ttl);
    }
}
