package io.stargate.sdk.data.client;

import java.io.Closeable;
import java.util.Iterator;
import java.util.function.Consumer;

/**
 * Cursor to iterate on a page.
 *
 * @param <TResult>
 *     current document.
 */
public interface DataApiCursor<TResult> extends Iterator<TResult>, Closeable {

    /**
     * {@link #close()} is allowed to be called concurrently with any method of the cursor, including itself.
     * This is useful to cancel blocked {@link #hasNext()}, {@link #next()}.
     */
    @Override
    void close();

    /**
     * Check if there is another element.
     *
     * @return
     *     if next element is present
     */
    @Override
    boolean hasNext();

    /**
     * Access next element if exists.
     *
     * @return
     *      next element.
     */
    @Override
    TResult next();

    /**
     * Gets the number of results available locally without blocking, which may be 0.
     *
     * @return
     *  If the cursor is known to be exhausted, returns 0.  If the cursor is closed before it's been exhausted, it may return a non-zero
     */
    int available();

    /**
     * Check each remaining
     *
     * @param action
     *      The action to be performed for each element
     */
    @Override
    default void forEachRemaining(final Consumer<? super TResult> action) {
        try {
            Iterator.super.forEachRemaining(action);
        } finally {
            close();
        }
    }

}
