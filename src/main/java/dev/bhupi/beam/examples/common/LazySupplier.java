package dev.bhupi.beam.examples.common;

import java.util.function.Supplier;

/**
 * A {@link Supplier} similar to the one returned by the {@link
 * com.google.common.base.Suppliers#memoize(com.google.common.base.Supplier)}, except that the
 * delegate supplier is passed on in the getter instead of during the construction.
 *
 * <p>It is needed to delay the actual construction of the supplied object, while still being able
 * to assign the empty LazySupplier to a static field in a thread-safe manner.
 *
 * @param <T> type of the supplied element
 */
public class LazySupplier<T> {

  private volatile T t;

  public T get(Supplier<T> supplier) {
    if (t == null) {
      synchronized (this) {
        if (t == null) {
          t = supplier.get();
        }
      }
    }
    return t;
  }
}
