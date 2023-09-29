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
public class QueryBuilder {

    static final ObjectMapper JACKSON_MAPPER = new ObjectMapper();

    // -----------------------------------
    // --     Working with Project     ---
    // -----------------------------------

    /**
     * Returned Map
     */
    public Map<String, Object> projection;

    public QueryBuilder select(String... keys) {
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

    public QueryBuilder selectVector() {
        return select(FilterKeyword.VECTOR.getKeyword());
    }

    public QueryBuilder selectSimilarity() {
        return select(FilterKeyword.SIMILARITY.getKeyword());
    }


    // -----------------------------------
    // --     Working with Sort        ---
    // -----------------------------------

    public Map<String, Object> sort;

    public QueryBuilder orderBy(String key, Object value) {
        if (null == sort) {
            sort = new HashMap<>();
        }
        sort.put(key, value);
        return this;
    }

    public QueryBuilder orderByAnn(Double... vector) {
        return orderBy(FilterKeyword.VECTOR.getKeyword(), vector);
    }

    public QueryBuilder orderByAnn(String textFragment) {
        return orderBy(FilterKeyword.VECTORIZE.getKeyword(), textFragment);
    }

    // -----------------------------------
    // --     Working with Options     ---
    // -----------------------------------

    /**
     * Returned Map
     */
    public Map<String, Object> options;

    public QueryBuilder limit(int limit) {
        if (null == options) {
            options = new HashMap<>();
        }
        options.put("limit", limit);
        return this;
    }


    // -----------------------------------
    // --     Working with Filter      ---
    // -----------------------------------

    /**
     * Returned Map
     */
    public Map<String, Object> filter;

    @SuppressWarnings("unchecked")
    public QueryBuilder withJsonFilter(String jsonFilter) {
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
    public QueryFilterBuilder where(String fieldName) {
        Assert.hasLength(fieldName, "fieldName");
        if (filter != null) {
            throw new IllegalArgumentException("Invalid query please use and() " +
                    "as a where clause has been provided");
        }
        filter = new HashMap<>();
        return new QueryFilterBuilder(this, fieldName);
    }

    /**
     * Only return those fields if provided.
     *
     * @param fieldName
     *          field name
     * @return SearchDocumentWhere
     *          current builder
     */
    public QueryFilterBuilder andWhere(String fieldName) {
        Assert.hasLength(fieldName, "fieldName");
        if (filter == null || filter.isEmpty()) {
            throw new IllegalArgumentException("Invalid query please use where() " +
                    "as a where clause has been provided");
        }
        return new QueryFilterBuilder(this, fieldName);
    }

    // -------------------------------
    // --    Final Builder         ---
    // -------------------------------

    /**
     * Terminal call to build immutable instance of {@link Query}.
     *
     * @return
     *      immutable instance of {@link Query}.
     */
    public Query build() {
        return new Query(this);
    }
    
}
