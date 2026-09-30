package ch.epfl.coshim.core;

/**
 * Thrown by {@link CoShim#get} / {@link CoShim#put} when the running transaction must abort (lock timeout, cascade,
 * lost upgrade race). The shim has already released the transaction's locks when this is thrown.
 */
public class TxnAbortedException extends RuntimeException {

    public TxnAbortedException(String message) {
        super(message);
    }
}
