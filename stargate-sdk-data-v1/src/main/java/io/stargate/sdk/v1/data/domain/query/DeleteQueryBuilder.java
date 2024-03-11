package io.stargate.sdk.v1.data.domain.query;

import io.stargate.sdk.http.domain.FilterKeyword;
import io.stargate.sdk.http.domain.FilterOperator;
import io.stargate.sdk.utils.JsonUtils;

import java.util.HashMap;
import java.util.Map;

/**
 * Helper to build queries
 */
public class DeleteQueryBuilder {

    /**
     * Default constructor.
     */
    public DeleteQueryBuilder() {
    }

    // -----------------------------------
    // -- Sort: 'order by'             ---
    // -----------------------------------

    /**
     * Sort field.
     */
    public Map<String, Object> sort;

    /**
     * Builder pattern.
     *
     * @param key
     *      add a key
     * @param value
     *      add value
     * @return
     *      self reference
     */
    public DeleteQueryBuilder orderBy(String key, Object value) {
        if (null == sort) {
            sort = new HashMap<>();
        }
        sort.put(key, value);
        return this;
    }

    /**
     * Builder pattern.
     *
     * @param vector
     *      vector for sor
     * @return
     *      self reference
     */
    public DeleteQueryBuilder orderByAnn(float[] vector) {
        return orderBy(FilterKeyword.VECTOR.getKeyword(), vector);
    }

    /**
     * Builder pattern.
     *
     * @param textFragment
     *      text to add for vectorize
     * @return
     *      self reference
     */
    public DeleteQueryBuilder orderByAnn(String textFragment) {
        return orderBy(FilterKeyword.VECTORIZE.getKeyword(), textFragment);
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
     *
     * @param jsonFilter
     *      filter
     * @return
     *      reference to self
     */
    @SuppressWarnings("unchecked")
    public DeleteQueryBuilder filter(String jsonFilter) {
        this.filter = JsonUtils.unmarshallBean(jsonFilter, Map.class);
        return this;
    }

    /**
     * Full filter as a filter object
     *
     * @param pFilter
     *      filter
     * @return
     *      reference to self
     */
    public DeleteQueryBuilder filter(Filter pFilter) {
        if (pFilter == null) return this;
        if (filter == null) {
            filter = new HashMap<>();
        }
        filter.putAll(pFilter.filter);
        return this;
    }

    /**
     * Full filter as a json string.
     * @param fieldName
     *      name of the filter
     * @param op
     *      operator
     * @param value
     *      simple filter
     * @return
     *      reference to self
     */
    public DeleteQueryBuilder where(String fieldName, FilterOperator op, Object value) {
        return filter(new Filter(fieldName, op, value));
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
    public DeleteQuery build() {
        return new DeleteQuery(this);
    }
    
}
