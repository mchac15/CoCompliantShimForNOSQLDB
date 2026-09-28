package ch.epfl.coshim.seata;

/**
 * Phase 1 of a shim branch failed (execute FAILED, prepare voted NO, or the TC rolled back). The
 * branch was reported as {@code PhaseOne_Failed}; the application should roll the global
 * transaction back (letting this propagate out of a {@code @GlobalTransactional} method does that).
 */
public class ShimBranchException extends RuntimeException {

    public ShimBranchException(String message) {
        super(message);
    }

    public ShimBranchException(String message, Throwable cause) {
        super(message, cause);
    }
}
