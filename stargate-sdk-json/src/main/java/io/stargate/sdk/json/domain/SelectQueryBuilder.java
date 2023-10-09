package io.stargate.sdk.json.domain;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.sdk.http.domain.FilterKeyword;
import io.stargate.sdk.utils.Assert;

import java.util.HashMap;
import java.util.Map;

/**
 * Helper to build queries
 */
public class SelectQueryBuilder {

    /**
     * Json Marshalling.
     */
    static final ObjectMapper JACKSON_MAPPER = new ObjectMapper();

    // -----------------------------------
    // -- Projection: 'select'         ---
    // -----------------------------------

    /**
     * Returned Map
     */
    public Map<String, Object> projection;

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

    public SelectQueryBuilder selectVector() {
        return select(FilterKeyword.VECTOR.getKeyword());
    }

    public SelectQueryBuilder selectSimilarity() {
        return select(FilterKeyword.SIMILARITY.getKeyword());
    }

    // -----------------------------------
    // -- Sort: 'order by'             ---
    // -----------------------------------

    public Map<String, Object> sort;

    public SelectQueryBuilder orderBy(String key, Object value) {
        if (null == sort) {
            sort = new HashMap<>();
        }
        sort.put(key, value);
        return this;
    }

    public SelectQueryBuilder orderByAnn(Float... vector) {
        return orderBy(FilterKeyword.VECTOR.getKeyword(), vector);
    }

    public SelectQueryBuilder orderByAnn(String textFragment) {
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
    public SelectQueryBuilder withLimit(int limit) {
        return withOption("limit", limit);
    }

    /**
     * Max result.
     *
     * @param limit
     *      maximum number of returned object
     * @return
     *      number of items
     */
    public SelectQueryBuilder withPageSize(int limit) {
        return withLimit(limit);
    }

    /**
     * Max result.
     *
     * @param skip
     *      maximum number of returned object
     * @return
     *      number of items
     */
    public SelectQueryBuilder withSkip(int skip) {
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
        return withOption("pagingState", pagingState);
    }

    /**
     * Paging State
     *
     * @return
     *     current builder
     */
    public SelectQueryBuilder withIncludeSimilarity() {
        return withOption("includeSimilarity", "true");
    }

    /**
     * Add an option to the request
     * @param key
     *      current key
     * @param value
     *       current value
     * @return
     *      reference to self
     */
    protected SelectQueryBuilder withOption(String key, Object value)  {
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
     * @param jsonFilter
     *      filter
     * @return
     *      reference to self
     */
    @SuppressWarnings("unchecked")
    public SelectQueryBuilder withJsonFilter(String jsonFilter) {
        try {
            this.filter = JACKSON_MAPPER.readValue(jsonFilter, Map.class);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Cannot parse json", e);
        }
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
