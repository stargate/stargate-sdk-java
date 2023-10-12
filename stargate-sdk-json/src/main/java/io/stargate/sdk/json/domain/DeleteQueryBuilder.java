package io.stargate.sdk.json.domain;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.sdk.http.domain.FilterKeyword;
import io.stargate.sdk.utils.Assert;
import io.stargate.sdk.utils.JsonUtils;
import lombok.NonNull;

import java.util.HashMap;
import java.util.List;
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
    public DeleteQueryBuilder withJsonFilter(String jsonFilter) {
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
    public DeleteQueryFilterBuilder where(String fieldName) {
        Assert.hasLength(fieldName, "fieldName");
        if (filter != null) {
            throw new IllegalArgumentException("Invalid query please use and() " +
                    "as a where clause has been provided");
        }
        filter = new HashMap<>();
        return new DeleteQueryFilterBuilder(this, fieldName);
    }

    /**
     * Only return those fields if provided.
     *
     * @param fieldName
     *          field name
     * @return SearchDocumentWhere
     *          current builder
     */
    public DeleteQueryFilterBuilder andWhere(String fieldName) {
        Assert.hasLength(fieldName, "fieldName");
        if (filter == null || filter.isEmpty()) {
            throw new IllegalArgumentException("Invalid query please use where() " +
                    "as a where clause has been provided");
        }
        return new DeleteQueryFilterBuilder(this, fieldName);
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
