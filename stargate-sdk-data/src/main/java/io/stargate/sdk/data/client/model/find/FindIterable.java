package io.stargate.sdk.data.client.model.find;

import io.stargate.sdk.core.domain.Page;
import io.stargate.sdk.data.client.DataApiCollection;
import io.stargate.sdk.data.client.model.DataApiPagedIterator;
import io.stargate.sdk.data.client.model.Document;
import io.stargate.sdk.data.client.model.Filter;
import lombok.Getter;
import lombok.NonNull;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

/**
 *
 * @param <DOC>
 */
@Getter
public class FindIterable<DOC> implements Iterable<DOC>, Closeable {

    // -------- Inputs ---------

    /** Reference to the collection in use. */
    private final DataApiCollection<DOC> collection;

    /** Original command, we will edit it to iterate on pages. */
    private final Filter filter;

    /** Original command, we will edit it to iterate on pages. */
    private final FindOptions options;

    // General Status

    /** Check host many has been processed (skip & limit support) */
    private final AtomicInteger totalItemProcessed = new AtomicInteger(0);

    /** The iterable is active and progressing on the results. */
    boolean active = false;

    /** the Iterator is exhausted */
    boolean exhausted = false;

    // ----- Page Informations ----

    Page<DOC> currentPage;

    int currentPageAvailable;

    DataApiPagedIterator<DOC> currentPageIterator;

    /**
     * Constructor for a cursor over the elements of the find.
     * @param collection
     *      source collection client, use to fetch next pages
     * @param filter
     *      original filter used to renew the query
     * @param options
     *      list of options like the pageState, limit of skip
     */
    public FindIterable(DataApiCollection<DOC> collection, Filter filter, FindOptions options) {
        this.collection  = collection;
        this.filter       = filter;
        this.options      = options;
    }

    /** {@inheritDoc} */
    @Override @NonNull
    public DataApiPagedIterator<DOC> iterator() {
        if (currentPageIterator == null) {
            active = fetchNextPage();
            this.currentPageIterator = new DataApiPagedIterator<DOC>(this);
        }
        return currentPageIterator;
    }

    /**
     * Fetch the next page if the result.
     *
     * @return
     *      if a new page has been found.
     */
    public boolean fetchNextPage() {
        if (currentPage == null || currentPage.getPageState().isPresent()) {
            if (currentPage != null) {
                options.withPageState(currentPage.getPageState().get());
            }
            this.currentPage  = collection.findPage(filter, options);
        }
        return false;
    }

    /**
     * When no more items available.
     */
    @Override
    public void close() {
        active    = false;
        exhausted = true;
    }

    /**
     * Trigger a specialized Api call with proper 'skip' and 'limit' to only collect the item that is missing.
     *
     * @param offset
     *      offset of the required items
     * @return
     *     tem if it exists
     */
    public Optional<DOC> getItem(int offset) {
        FindOptions singleResultOptions = new FindOptions();
        singleResultOptions.skip(offset);
        singleResultOptions.limit(1);
        if (options.getIncludeSimilarity()) {
            singleResultOptions.includeSimilarity();
        }
        FindIterable<DOC> sub = new FindIterable<>(collection, filter, singleResultOptions);
        if (sub.fetchNextPage() && sub.getCurrentPage() != null && !sub.getCurrentPage().getResults().isEmpty()) {
            return Optional.ofNullable(sub.getCurrentPage().getResults().get(0));
        }
        return Optional.empty();
    }

    /**
     * Helper to return the first item in the iterator or null.
     *
     * @return T the first item or null.
     */
     public Optional<DOC> first() {
         return getItem(0);
     }

    /**
     * Will exhaust the list and put all value in memory.
     *
     * @return
     *      all values of the iterable
     */
     public List<DOC> all() {
         if (exhausted) throw new IllegalStateException("Iterable is already exhauted.");
         if (active)    throw new IllegalStateException("Iterable has already been started");
         List<DOC> results = new ArrayList<>();
         for (DOC doc : this) results.add(doc);
         return results;
     }

}
