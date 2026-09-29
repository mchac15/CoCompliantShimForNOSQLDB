package ch.epfl.coshim.jdbc;

import java.time.Duration;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * XA-layer bookkeeping for one race that pseudo.txt excludes by assumption, but Seata does not.
 *
 * <p>pseudo.txt (assumption 3) lets the coordinator send nothing for a txn before its execute, and
 * keeps no tombstones. Seata's TC gives no such guarantee: a rollback (e.g. global timeout) can reach
 * the RM after {@code branchRegister} but before {@code xa start}, or while phase 1 still runs. The
 * shim's abort is then a no-op (unknown id) or lands too early, and the branch would go on, vote YES
 * and keep its locks forever. This class remembers such rollbacks so the branch is refused/aborted.
 *
 * <p>TODO: this is a first mitigation, not the final design. It lives outside the protocol and uses
 * expiring tombstones (see {@link #pruneTombstones}). A further improvement is to handle the
 * "decision before execute" case in the shim protocol itself (pseudo.txt), or to prove it cannot
 * happen with Seata's XA flow, and then drop this class.
 *
 * <p>These are <b>not</b> the transaction statuses of pseudo.txt. {@code started}, {@code executed},
 * {@code prepared}, {@code committing}, {@code committed}, {@code aborted} and {@code must_abort} live
 * in the shim ({@code SpeculativeCoShim}'s transactions_map). This class only tracks, per XA branch,
 * which XA phase the branch is in and whether the TC's rollback arrived during it:
 *
 * <pre>
 *   branch phase          shim statuses during it              set by
 *   IN_PHASE_ONE          started, executed, must_abort        xa start
 *   VOTED_YES             prepared, committing                 xa prepare returned XA_OK
 *   ROLLBACK_RECEIVED     (any, or txn not registered yet)     TC rollback during phase 1 / before start
 *   (no entry)            committed, aborted, unknown id       commit, local rollback, failed phase 1
 * </pre>
 *
 * The shim's own aborts ({@code aborted}, {@code must_abort} from a cascade, lock timeout) need no
 * state here: the XA layer learns about them from the shim's answers (get/put throws, end returns
 * FAILED, prepare votes NO) and drops the entry.
 */
final class BranchStates {

    static final Duration TOMBSTONE_TTL = Duration.ofMinutes(10);

    private enum Phase { IN_PHASE_ONE, VOTED_YES, ROLLBACK_RECEIVED }

    private record Entry(Phase phase, long sinceNanos) {}

    private final ConcurrentMap<String, Entry> branches = new ConcurrentHashMap<>();

    /** Before {@code shim.start}. @return false if the TC already rolled the branch back. */
    boolean activate(String txnId) {
        Entry e = branches.compute(
                txnId, (k, cur) -> cur == null ? new Entry(Phase.IN_PHASE_ONE, System.nanoTime()) : cur);
        if (e.phase == Phase.ROLLBACK_RECEIVED) {
            branches.remove(txnId);
            return false;
        }
        return true;
    }

    /** After each phase-1 step: did the TC roll the branch back meanwhile? */
    boolean isRolledBack(String txnId) {
        Entry e = branches.get(txnId);
        return e != null && e.phase == Phase.ROLLBACK_RECEIVED;
    }

    /**
     * After the shim voted YES, atomically with the check for a TC rollback (a rollback landing
     * between a separate check and this update would otherwise leave a stale entry).
     *
     * @return true if the branch is now VOTED_YES; false if the TC rolled it back meanwhile, in which
     *     case the entry is removed and the caller must abort the txn and fail the prepare
     */
    boolean votedYes(String txnId) {
        boolean[] rolledBack = {false};
        branches.computeIfPresent(txnId, (k, cur) -> {
            if (cur.phase == Phase.ROLLBACK_RECEIVED) {
                rolledBack[0] = true;
                return null;
            }
            return new Entry(Phase.VOTED_YES, cur.sinceNanos);
        });
        return !rolledBack[0];
    }

    /** The branch is over (commit, local rollback, or a failed phase 1 seen by its own thread). */
    void finished(String txnId) {
        branches.remove(txnId);
    }

    /**
     * A rollback coming from the TC (phase 2, or racing phase 1). The caller then calls shim.abort.
     *
     * <pre>
     *   current entry        -> new entry           why
     *   VOTED_YES            -> removed             normal phase 2: the shim knows the txn, its abort is enough
     *   IN_PHASE_ONE         -> ROLLBACK_RECEIVED   phase 1 still running; the shim may not have registered the
     *                                               txn yet (window inside xa start), so the phase-1 thread must
     *                                               re-check and abort itself (start / votedYes)
     *   ROLLBACK_RECEIVED    -> ROLLBACK_RECEIVED   duplicate/retried rollback, timestamp refreshed
     *   none                 -> ROLLBACK_RECEIVED   before xa start (tombstone), or a retried rollback of a
     *                                               finished branch (expires, see pruneTombstones)
     * </pre>
     */
    void rolledBackByCoordinator(String txnId) {
        long now = System.nanoTime();
        branches.compute(txnId, (k, cur) -> cur != null && cur.phase == Phase.VOTED_YES
                ? null
                : new Entry(Phase.ROLLBACK_RECEIVED, now));
        pruneTombstones(now);
    }

    int size() {
        return branches.size();
    }

    /**
     * Drops ROLLBACK_RECEIVED entries older than {@link #TOMBSTONE_TTL}, so tombstones of branches that
     * never start again (e.g. a retried TC rollback of an already finished branch) do not accumulate.
     *
     * <p>WARNING: not definitive, this can lead to an inconsistent state. If the {@code xa start} of a
     * rolled-back branch arrives after its tombstone expired (a phase 1 stalled longer than the TTL),
     * the branch is accepted, can vote YES and keeps its locks although the global transaction was
     * rolled back. The TTL only makes this unlikely. See the TODO on the class.
     */
    private void pruneTombstones(long now) {
        long ttl = TOMBSTONE_TTL.toNanos();
        branches.entrySet().removeIf(
                en -> en.getValue().phase == Phase.ROLLBACK_RECEIVED && now - en.getValue().sinceNanos > ttl);
    }
}
