package io.stargate.sdk.data.client.model;

import io.stargate.sdk.core.domain.Page;
import io.stargate.sdk.data.client.model.find.FindIterable;
import lombok.Getter;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Implementing a custom iterator that will load next page if needed when hitting the last item of page.
 *
 * @param <DOC>
 *     working document
 */
@Getter
public class DataApiPagedIterator<DOC> implements Iterator<DOC> {

    private final FindIterable<DOC> parentIterable;

    /** Progress on the current page. */
    private int availableWithoutFetch;

    /** Iterator on current document page. */
    private Iterator<DOC> resultsIterator;

    /**
     * Starting the cursor on an iterable to fetch more pages.
     *
     * @param findIterable
     *      iterable
     */
    public DataApiPagedIterator(FindIterable<DOC> findIterable) {
        this.parentIterable        = findIterable;
        this.availableWithoutFetch = findIterable.getCurrentPage().getResults().size();
        this.resultsIterator       = findIterable.getCurrentPage().getResults().iterator();
    }

    /**
     * Trigger the load of the next page.
     *
     * @return
     *     fetch next page
     */
    private boolean fetchNextPage() {
        if (parentIterable.fetchNextPage()) {
            Page<DOC> newPage = parentIterable.getCurrentPage();
            this.availableWithoutFetch = newPage.getResults().size();
            this.resultsIterator = newPage.getResults().iterator();
            return true;
        }
        return false;
    }

    /** {@inheritDoc} */
    @Override
    public boolean hasNext() {
        boolean hasNext =  (resultsIterator.hasNext() || parentIterable.getCurrentPage().getPageState().isPresent());
        if (!hasNext) {
            parentIterable.close();
        }
        return hasNext;
    }

    /**
     * Implementing a logic of iterator combining current page and paging. An local iterator is started on elements
     * of the processing page. If the local iterator is exhausted, the flag 'nextPageState' can tell us is there are
     * more elements to retrieve. if 'nextPageState' is not null the next page is fetch at Iterable level and the
     * local iterator is reinitialized on the new page.
     *
     * @return
     *      next document in the iterator
     */
    @Override
    public DOC next() {
        if (resultsIterator.hasNext()) {
            availableWithoutFetch--;
            parentIterable.getTotalItemProcessed().incrementAndGet();
            return resultsIterator.next();
        } else if (parentIterable.getCurrentPage().getPageState().isPresent()) {
            parentIterable.fetchNextPage();
            this.availableWithoutFetch = parentIterable.getCurrentPage().getResults().size();
            this.resultsIterator = parentIterable.getCurrentPage().getResults().iterator();
            return next();
        }
        throw new NoSuchElementException("Current page is exhausted and no new page available");
    }

    /**
     * Gets the number of results available locally without blocking, which may be 0.
     *
     * <p>
     * If the cursor is known to be exhausted, returns 0.  If the cursor is closed before it's been exhausted, it may return a non-zero
     * value.
     * </p>
     *
     * @return the number of results available locally without blocking
     * @since 4.4
     */
    public int available() {
        return 0;
    }
}
