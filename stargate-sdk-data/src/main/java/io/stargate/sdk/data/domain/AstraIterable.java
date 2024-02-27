package io.stargate.sdk.data.domain;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public interface AstraIterable<TResult> extends Iterable<TResult> {

    AstraIterable<TResult> batchSize(int batchSize);

    default TResult one() {
        Iterator<TResult> iterator = this.iterator();
        return iterator.hasNext() ? iterator.next() : null;
    }

    default List<TResult> all() {
        if (!this.iterator().hasNext()) {
            return Collections.emptyList();
        } else {
            // not parallel
            return StreamSupport
                    .stream(this.spliterator(), false)
                    .collect(Collectors.toList());
        }
    }

    boolean isFullyFetched();

    int getAvailableWithoutFetching();

    default <U> AstraIterable<U> map(Function<TResult,U> mapper) {
        return new AstraIterable<U>() {
            @Override
            public AstraIterable<U> batchSize(int batchSize) {
                return AstraIterable.this.batchSize(batchSize).map(mapper);
            }

            @Override
            public Iterator<U> iterator() {
                return StreamSupport
                        .stream(AstraIterable.this.spliterator(), false)
                        .map(mapper)
                        .iterator();
            }

            @Override
            public boolean isFullyFetched() {
                return AstraIterable.this.isFullyFetched();
            }

            @Override
            public int getAvailableWithoutFetching() {
                return 0;
            }
        };
    }

}
