package io.stargate.sdk.data.domain;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * Building an Iterable to interact with Api.
 *
 * @param <DOC>
 *          parameterized type
 */
public interface ResultIterable<DOC> extends Iterable<DOC> {

    /**
     * Enable to change the Batch Size.
     *
     * @param batchSize
     *      batch size
     * @return
     *      current instance
     */
    ResultIterable<DOC> batchSize(int batchSize);

    /**
     * Return the first element.
     *
     * @return
     *      first element
     */
    default DOC one() {
        Iterator<DOC> iterator = this.iterator();
        return iterator.hasNext() ? iterator.next() : null;
    }

    /**
     * Return all elements.
     *
     * @return
     *      all elements
     */
    default List<DOC> all() {
        if (!this.iterator().hasNext()) {
            return Collections.emptyList();
        } else {
            // not parallel
            return StreamSupport
                    .stream(this.spliterator(), false)
                    .collect(Collectors.toList());
        }
    }

    /**
     * Return the first element.
     *
     * @return
     *      first element
     */
    boolean isFullyFetched();

    /**
     * Map the result to another type.
     *
     * @param mapper
     *      mapper
     * @param <U>
     *      new type
     * @return
     *      new instance
     */
    default <U> ResultIterable<U> map(Function<DOC,U> mapper) {
        return new ResultIterable<U>() {
            /** {@inheritDoc} */
            @Override
            public ResultIterable<U> batchSize(int batchSize) {
                return ResultIterable.this.batchSize(batchSize).map(mapper);
            }

            /** {@inheritDoc} */
            @Override
            public Iterator<U> iterator() {
                return StreamSupport
                        .stream(ResultIterable.this.spliterator(), false)
                        .map(mapper)
                        .iterator();
            }

            /** {@inheritDoc} */
            @Override
            public boolean isFullyFetched() {
                return ResultIterable.this.isFullyFetched();
            }
        };
    }

}
