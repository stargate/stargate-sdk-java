package io.stargate.sdk.data.client;

import java.util.Collection;
import java.util.Optional;
import java.util.function.Function;

/**
 * Iterable to work on returned object (fetching pages).
 *
 * @param <TResult>
 *          working document
 */
public interface DataApiIterable <TResult> extends Iterable<TResult> {

    @Override
    DataApiCursor<TResult> iterator();

    /**
     * Returns a cursor used for iterating over elements of type {@code TResult}. The cursor is primarily used for change streams.
     *
     * @return a cursor
     * @since 3.11
     */
    DataApiCursor<TResult> cursor();

    /**
     * Helper to return the first item in the iterator or null.
     *
     * @return T the first item if exists.
     */
    Optional<TResult> first();

    /**
     * Maps this iterable from the source document type to the target document type.
     *
     * @param mapper a function that maps from the source to the target document type
     * @param <U>    the target document type
     * @return an iterable which maps T to U
     */
    <U> DataApiIterable<U> map(Function<TResult, U> mapper);

    /**
     * Iterates over all the documents, adding each to the given target.
     *
     * @param target the collection to insert into
     * @param <A>    the collection type
     * @return the target
     */
    <A extends Collection<? super TResult>> A into(A target);

    /**
     * Sets the number of documents to return per batch.
     *
     * @param batchSize
     *      the batch size
     * @return
     *      this
     */
    DataApiIterable<TResult> batchSize(int batchSize);

}