package io.stargate.sdk.json.domain;

import io.stargate.sdk.http.domain.FilterKeyword;
import io.stargate.sdk.utils.Assert;
import io.stargate.sdk.utils.JsonUtils;
import lombok.NonNull;

import java.util.HashMap;
import java.util.Map;

/**
 * Helper to build queries
 */
public class SelectQueryBuilder {

    // -----------------------------------
    // -- Projection: 'select'         ---
    // -----------------------------------

    /**
     * Returned Map
     */
    public Map<String, Object> projection;

    /**
     * Default constructor.
     */
    public SelectQueryBuilder() {
    }

    /**
     * List of fields to be returned.
     *
     * @param keys
     *      keys
     * @return
     *     reference to the builder
     */
    public SelectQueryBuilder select(String... keys) {
        if (null == projection) {
            projection = new HashMap<>();
        }
        if (keys != null) {
            for (String key : keys) {
                projection.put(key, 1);
            }
        }
        return this;
    }

    // -----------------------------------
    // -- Sort: 'order by'             ---
    // -----------------------------------

    /**
     * order by.
     */
    public Map<String, Object> sort;

    /**
     * Builder Pattern
     *
     * @param key
     *      updated key
     * @param value
     *      updated value
     * @return
     *      self reference
     */
    public SelectQueryBuilder orderBy(String key, Object value) {
        if (null == sort) {
            sort = new HashMap<>();
        }
        sort.put(key, value);
        return this;
    }

    /**
     * Builder Pattern
     *
     * @param vector
     *      add vector in the order by
     * @return
     *      self reference
     */
    public SelectQueryBuilder orderByAnn(float[] vector) {
        if (vector == null) return this;
        return orderBy(FilterKeyword.VECTOR.getKeyword(), vector);
    }

    /**
     * Builder Pattern
     *
     * @param textFragment
     *      add text in the order by (vectorize)
     * @return
     *      self reference
     */
    public SelectQueryBuilder orderByAnn(@NonNull String textFragment) {
        return orderBy(FilterKeyword.VECTORIZE.getKeyword(), textFragment);
    }

    // -----------------------------------
    // --  Options: limit...          ---
    // -----------------------------------

    /**
     * Returned Map
     */
    public Map<String, Object> options;

    /**
     * Max result.
     *
     * @param limit
     *      maximum number of returned object
     * @return
     *      number of items
     */
    public SelectQueryBuilder withLimit(Integer limit) {
        if (limit == null || limit > 20) return this;
        return withOption("limit", limit);
    }

    /**
     * Max result.
     *
     * @param skip
     *      maximum number of returned object
     * @return
     *      number of items
     */
    public SelectQueryBuilder withSkip(Integer skip) {
        if (skip == null) return this;
        return withOption("skip", skip);
    }

    /**
     * Paging State
     *
     * @param pagingState
     *      get second page
     * @return
     *      current builder
     */
    public SelectQueryBuilder withPagingState(String pagingState) {
        if (pagingState == null) return this;
        return withOption("pagingState", pagingState);
    }

    /**
     * Paging State
     *
     * @return
     *     current builder
     */
    public SelectQueryBuilder includeSimilarity() {
        return withOption("includeSimilarity", "true");
    }

    /**
     * Add an option to the request.
     *
     * @param key
     *      current key
     * @param value
     *       current value
     * @return
     *      reference to self
     */
    protected SelectQueryBuilder withOption(@NonNull String key, @NonNull Object value)  {
        if (null == options) options = new HashMap<>();
        options.put(key, value);
        return this;
    }

    // -----------------------------------
    // --     Working with Filter      ---
    // -----------------------------------

    /**
     * Returned Map
     */
    public Map<String, Object> filter;

    /**
     * Full filter as a json string.
     * @param pFilter
     *      filter
     * @return
     *      reference to self
     */
    @SuppressWarnings("unchecked")
    public SelectQueryBuilder withFilter(Filter pFilter) {
        if (pFilter == null) return this;
        if (filter == null) {
            filter = new HashMap<>();
        }
        filter.putAll(pFilter.filter);
        return this;
    }

    /**
     * Full filter as a json string.
     * @param jsonFilter
     *      filter
     * @return
     *      reference to self
     */
    @SuppressWarnings("unchecked")
    public SelectQueryBuilder withJsonFilter(String jsonFilter) {
        if (jsonFilter == null) return this;
        this.filter = JsonUtils.unmarshallBean(jsonFilter, Map.class);
        return this;
    }

    /**
     * Work with arguments.
     *
     * @param fieldName
     *      current field name.
     * @return
     *      builder for the filter
     */
    public SelectQueryFilterBuilder where(String fieldName) {
        Assert.hasLength(fieldName, "fieldName");
        if (filter != null) {
            throw new IllegalArgumentException("Invalid query please use and() " +
                    "as a where clause has been provided");
        }
        filter = new HashMap<>();
        return new SelectQueryFilterBuilder(this, fieldName);
    }

    /**
     * Only return those fields if provided.
     *
     * @param fieldName
     *          field name
     * @return SearchDocumentWhere
     *          current builder
     */
    public SelectQueryFilterBuilder andWhere(String fieldName) {
        Assert.hasLength(fieldName, "fieldName");
        if (filter == null || filter.isEmpty()) {
            throw new IllegalArgumentException("Invalid query please use where() " +
                    "as a where clause has been provided");
        }
        return new SelectQueryFilterBuilder(this, fieldName);
    }

    // -------------------------------
    // --    Final Builder         ---
    // -------------------------------

    /**
     * Terminal call to build immutable instance of {@link SelectQuery}.
     *
     * @return
     *      immutable instance of {@link SelectQuery}.
     */
    public SelectQuery build() {
        return new SelectQuery(this);
    }
    
}
