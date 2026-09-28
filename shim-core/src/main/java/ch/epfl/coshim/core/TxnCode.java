package ch.epfl.coshim.core;

/**
 * Client code of one subtransaction. It may only touch the store through {@code ctx}.
 *
 * <p>If {@link TxnContext#get} or {@link TxnContext#put} throws {@link TxnAbortedException} (the
 * FAILED result of pseudo.txt), the code must stop immediately. Letting the exception propagate is
 * the intended way to do that.
 */
@FunctionalInterface
public interface TxnCode<K, V> {

    void run(TxnContext<K, V> ctx);
}
