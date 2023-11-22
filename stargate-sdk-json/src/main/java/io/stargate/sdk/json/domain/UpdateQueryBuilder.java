package io.stargate.sdk.json.domain;

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
public class UpdateQueryBuilder {

    /**
     * Default constructor.
     */
    public UpdateQueryBuilder() {
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
    public UpdateQueryBuilder orderBy(String key, Object value) {
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
    public UpdateQueryBuilder orderByAnn(float[] vector) {
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
    public static enum ReturnDocument {
        /**
         * Return the document after the update is applied.
         */
        after,

        /**
         * Return the document before the update is applied.
         */
        before
    }

    /**
     * Specifies which document to perform the projection on.
     * If `before` the projection is performed on the document
     * before the update is applied, if `after` the document
     * projection is from the document after the update.
     *
     * @param returnDocument
     *      document returned
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
    public UpdateQueryBuilder enableUpsert() {
        return withOption("upsert", true);
    }

    /**
     * Add an option to the request,
     *
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
     *
     * @param jsonFilter
     *      filter
     * @return
     *      reference to self
     */
    @SuppressWarnings("unchecked")
    public UpdateQueryBuilder withJsonFilter(String jsonFilter) {
        this.filter = JsonUtils.unmarshallBean(jsonFilter, Map.class);
        return this;
    }

    /**
     * Full update as a json string.
     *
     * @param jsonUpdate
     *      content of the update as json
     * @return
     *      reference to self
     */
    @SuppressWarnings("unchecked")
    public UpdateQueryBuilder withJsonUpdate(String jsonUpdate) {
        this.update = JsonUtils.unmarshallBean(jsonUpdate, Map.class);
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
    public JsonDocument replacement;

    /**
     * Builder pattern
     *
     * @param replacement
     *      new value for document
     * @return
     *      reference to self
     */
    public UpdateQueryBuilder replaceBy(JsonDocument replacement) {
        this.replacement = replacement;
        return this;
    }

    /**
     * Builder pattern
     *
     * @param key
     *      field name
     * @param offset
     *      increment value
     * @return
     *      reference to self
     */
    public UpdateQueryBuilder updateInc(String key, Integer offset) {
        return update("$inc", key, offset);
    }

    /**
     * Builder pattern
     *
     * @param key
     *      field name
     * @return
     *      reference to self
     */
    public UpdateQueryBuilder updateUnset(String key) {
        return update("$unset", key, "");
    }

    /**
     * Builder pattern
     *
     * @param key
     *      field name
     * @param value
     *      filed value
     * @return
     *      reference to self
     */
    public UpdateQueryBuilder updateSet(String key, Object value) {
        return update("$set", key, value);
    }

    /**
     * Builder pattern
     *
     * @param key
     *      field name
     * @param value
     *      filed value
     * @return
     *      reference to self
     */
    public UpdateQueryBuilder updateMin(String key, Object value) {
       return update("$min", key, value);
    }

    /**
     * Builder pattern
     *
     * @param key
     *      field name
     * @param value
     *      filed value
     * @return
     *      reference to self
     */
    public UpdateQueryBuilder updatePush(String key, Object value) {
        return update("$push", key, value);
    }

    /**
     * Builder pattern.
     *
     * @param key
     *      field name
     * @param values
     *      filed list values
     * @param position
     *      where to push in the list
     * @return
     *      reference to self
     */
    public UpdateQueryBuilder updatePushEach(String key, List<Object> values, Integer position) {
        // The value need "$each"
        Map<String, Object> value = new HashMap<>();
        value.put("$each", values);
        if (null != position) {
            value.put("$position", position);
        }
        return update("$push", key, value);
    }

    /**
     * Builder pattern
     *
     * @param key
     *      field name
     * @param value
     *      filed value
     * @return
     *      reference to self
     */
    public UpdateQueryBuilder updateAddToSet(String key, Object value) {
        return update("$addToSet", key, value);
    }

    /**
     * Builder pattern
     *
     * @param fields
     *      fields to rename
     * @return
     *      reference to self
     */
    public UpdateQueryBuilder updateRename(@NonNull  Map<String, String> fields) {
        fields.forEach((key, value) -> update("$rename", key, value));
        return this;
    }

    /**
     * Builder pattern
     *
     * @param operation
     *      operation on update
     * @param key
     *      field name
     * @param value
     *      filed value
     * @return
     *      reference to self
     */
    @SuppressWarnings("unchecked")
    private UpdateQueryBuilder update(String operation, String key, Object value) {
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
