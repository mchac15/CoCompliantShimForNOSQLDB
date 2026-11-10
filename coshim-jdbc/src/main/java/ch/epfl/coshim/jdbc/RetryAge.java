package ch.epfl.coshim.jdbc;

/**
 * The age the shims give the next branch this thread starts ({@link CoShimXAResource#start}), for their
 * deadlock prevention: an application retrying a global transaction sets its first attempt's age, so that a
 * retry keeps its seniority (as a restarted txn keeps its timestamp in wound-wait and wait-die, which then never
 * starve it). Unset: the shim derives the age from the branch's own xid, as for a first attempt.
 */
public final class RetryAge {

    private static final ThreadLocal<Long> AGE = new ThreadLocal<>();

    private RetryAge() {}

    /** Set on the thread that starts the branch (Seata: the one calling {@code setAutoCommit(false)}). */
    public static void set(long age) {
        AGE.set(age);
    }

    public static void clear() {
        AGE.remove();
    }

    static Long get() {
        return AGE.get();
    }
}
