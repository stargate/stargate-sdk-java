package io.stargate.sdk.json.domain;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.sdk.http.domain.FilterKeyword;
import io.stargate.sdk.utils.Assert;
import lombok.NonNull;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Helper to build queries
 */
public class UpdateQueryBuilder {

    /**
     * Json Marshalling.
     */
    static final ObjectMapper JACKSON_MAPPER = new ObjectMapper();

    // -----------------------------------
    // -- Sort: 'order by'             ---
    // -----------------------------------

    public Map<String, Object> sort;

    public UpdateQueryBuilder orderBy(String key, Object value) {
        if (null == sort) {
            sort = new HashMap<>();
        }
        sort.put(key, value);
        return this;
    }

    public UpdateQueryBuilder orderByAnn(Float... vector) {
        return orderBy(FilterKeyword.VECTOR.getKeyword(), vector);
    }

    public UpdateQueryBuilder orderByAnn(String textFragment) {
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
     * Values for return document.
     */
    public static enum ReturnDocument { after, before}

    /**
     * Specifies which document to perform the projection on.
     * If `before` the projection is performed on the document
     * before the update is applied, if `after` the document
     * projection is from the document after the update.
     *
     * @return
     *     current builder
     */
    public UpdateQueryBuilder withReturnDocument(ReturnDocument returnDocument) {
        return withOption("returnDocument", returnDocument.name());
    }

    /**
     * Upsert: "When `true`, if no documents match the `filter` clause the command will
     * create a new _empty_ document and apply the `update` clause and all equality
     * filters to the empty document."
     *
     * @return
     *     current builder
     */
    public UpdateQueryBuilder withUpsert() {
        return withOption("upsert", true);
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
    protected UpdateQueryBuilder withOption(String key, Object value)  {
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
    public UpdateQueryBuilder withJsonFilter(String jsonFilter) {
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
    public UpdateQueryFilterBuilder where(String fieldName) {
        Assert.hasLength(fieldName, "fieldName");
        if (filter != null) {
            throw new IllegalArgumentException("Invalid query please use and() " +
                    "as a where clause has been provided");
        }
        filter = new HashMap<>();
        return new UpdateQueryFilterBuilder(this, fieldName);
    }

    /**
     * Only return those fields if provided.
     *
     * @param fieldName
     *          field name
     * @return SearchDocumentWhere
     *          current builder
     */
    public UpdateQueryFilterBuilder andWhere(String fieldName) {
        Assert.hasLength(fieldName, "fieldName");
        if (filter == null || filter.isEmpty()) {
            throw new IllegalArgumentException("Invalid query please use where() " +
                    "as a where clause has been provided");
        }
        return new UpdateQueryFilterBuilder(this, fieldName);
    }

    // -----------------------------------
    // -- Update: 'update'            ---
    // -----------------------------------

    /**
     * Returned Map
     */
    public Map<String, Object> update;

    /**
     * replacement recod
     */
    public JsonRecord replacement;

    public UpdateQueryBuilder replaceBy(JsonRecord replacement) {
        this.replacement = replacement;
        return this;
    }


    public UpdateQueryBuilder updateInc(String key, Integer offset) {
        return update("$inc", key, offset);
    }
    public UpdateQueryBuilder updateUnset(String key) {
        return update("$unset", key, "");
    }

    public UpdateQueryBuilder updateSet(String key, Object value) {
        return update("$set", key, value);
    }

    public UpdateQueryBuilder updateMin(String key, Object value) {
       return update("$min", key, value);
    }

    public UpdateQueryBuilder updatePush(String key, Object value) {
        return update("$push", key, value);
    }

    public UpdateQueryBuilder updatePushEach(String key, List<Object> values, Integer position) {
        // The value need "$each"
        Map<String, Object> value = new HashMap<>();
        value.put("$each", values);
        if (null != position) {
            value.put("$position", position);
        }
        return update("$push", key, value);
    }

    public UpdateQueryBuilder updateAddToSet(String key, Object value) {
        return update("$addToSet", key, value);
    }

    public UpdateQueryBuilder updateRename(@NonNull  Map<String, String> fields) {
        fields.forEach((key, value) -> update("$rename", key, value));
        return this;
    }

    @SuppressWarnings("unchecked")
    public UpdateQueryBuilder update(String operation, String key, Object value) {
        if (null == update) update = new HashMap<>();
        update.computeIfAbsent(operation, k -> new HashMap<>());
        ((Map<String, Object>) update.get(operation)).put(key, value);
        return this;
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
    public UpdateQuery build() {
        return new UpdateQuery(this);
    }
    
}
