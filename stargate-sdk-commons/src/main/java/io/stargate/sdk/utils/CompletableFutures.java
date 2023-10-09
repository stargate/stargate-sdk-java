package io.stargate.sdk.utils;

import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

/**
 * Utilities to work with Async functions.
 */
public class CompletableFutures {

    /**
     * Hide constructor in utilities.
     */
    private CompletableFutures() {}

    /**
     * Merge multiple CompletionStage in a single one
     * @param inputs
     *      list of completion stages
     * @return
     *      the merged stage
     * @param <T>
     *      generic used with stages
     */
    public static <T> CompletionStage<Void> allDone(List<CompletionStage<T>> inputs) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        if (inputs.isEmpty()) {
            result.complete(null);
        } else {
            final int todo = inputs.size();
            final AtomicInteger done = new AtomicInteger();
            for (CompletionStage<?> input : inputs) {
                input.whenComplete(
                        (v, error) -> {
                            if (done.incrementAndGet() == todo) {
                                result.complete(null);
                            }
                        });
            }
        }
        return result;
    }
}