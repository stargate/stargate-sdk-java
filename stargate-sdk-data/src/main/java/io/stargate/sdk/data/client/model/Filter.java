package io.stargate.sdk.data.client.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import io.stargate.sdk.http.domain.FilterOperator;
import io.stargate.sdk.utils.Assert;
import io.stargate.sdk.utils.JsonUtils;
import lombok.Getter;
import lombok.NonNull;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Filter Builder.
 */
@Getter
public class Filter {

    @JsonInclude(JsonInclude.Include.NON_NULL)
    Map<String, Object> filter = new LinkedHashMap<>();

    /**
     * Default constructor.
     */
    public Filter() {}

    /**
     * Default constructor.
     *
     * @param json
     *      filter expression as JSON
     */
    @SuppressWarnings("unchecked")
    public Filter(String json) {
        this.filter = JsonUtils.unmarshallBean(json, Map.class);
    }

    /**
     * Default constructor.
     *
     * @param obj
     *      filter expression as JSON
     */
    public Filter(Map<String, Object> obj) {
        this.filter = obj;
    }

    /**
     * Create a filter from a where clause.
     *
     * @param fieldName
     *      fieldName
     * @param cond
     *      condition
     * @param value
     *      object value
     */
    public Filter(@NonNull String fieldName, @NonNull FilterOperator cond, @NonNull Object value) {
        this.filter.put(fieldName, Map.of(cond.getOperator(), value));
    }

    /**
     * Work with arguments.
     *
     * @param fieldName
     *      current field name.
     * @return
     *      builder for the filter
     */
    public FilterBuilder where(String fieldName) {
        Assert.hasLength(fieldName, "fieldName");
        filter = new HashMap<>();
        return new FilterBuilder(this, fieldName);
    }

    /**
     * Build where clause with operator
     *
     * @param fieldName
     *      current field name
     * @param cond
     *      current condition
     * @param value
     *      value for the condition
     * @return
     *      current
     */
    public Filter where(String fieldName, FilterOperator cond, Object value) {
        filter.put(fieldName, Map.of(cond.getOperator(), value));
        return this;
    }

    /**
     * Adding a ADD keyword.
     *
     * @return
     *      current list
     */
    public FilterBuilderList and() {
        return new FilterBuilderList(this, "$and");
    }

    /**
     * Adding a OR keyword.
     *
     * @return
     *      current list
     */
    public FilterBuilderList or() {
        return new FilterBuilderList(this, "$or");
    }

    /**
     * Adding a NOT keyword.
     *
     * @return
     *      current list
     */
    public FilterBuilderList not() {
        return new FilterBuilderList(this, "$not");
    }

    /**
     * Build a filter for find by id.
     *
     * @param id
     *      identifier
     * @return
     *      filter
     */
    public static Filter findById(String id) {
        return new Filter().where("_id").isEqualsTo(id);
    }

    /* {@inheritDoc} */
    @Override
    public String toString() {
       return toJson();
    }

    /**
     * Express the json filter as a string.
     *
     * @return
     *      json expression
     */
    public String toJson() {
        return JsonUtils.marshallForDataApi(this);
    }


}
