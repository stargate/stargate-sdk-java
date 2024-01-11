package io.stargate.sdk.data.domain.query;

import io.stargate.sdk.http.domain.FilterKeyword;
import io.stargate.sdk.http.domain.FilterOperator;

import java.util.Map;

/**
 * Helper to build a where clause in natural language (fluent API).
 * 
 * @author Cedrick LUNVEN (@clunven)
 */
public class FilterBuilder {

    /** Required field name. */
    private final String fieldName;

    /** Working builder to override the 'where' field and move with builder. */
    private final Filter filter;

    /**
     * Only constructor allowed
     *
     * @param filter
     *  sample filter
     * @param fieldName
     *      field name
     */
    protected FilterBuilder(Filter filter, String fieldName) {
        this.filter    = filter;
        this.fieldName = fieldName;
    }

    /**
     * Syntax sugar.
     * @param cond
     *      conditions
     * @param value
     *      value
     * @return
     *      builder
     */
    private Filter simpleOperator(FilterOperator cond, Object value) {
        filter.filter.put(fieldName, Map.of(cond.getOperator(), value));
        return filter;
    }

    /**
     * Syntax sugar.
     *
     * @param key
     *      keyword (size, exists)
     * @param value
     *      value
     * @return
     *      builder
     */
    private Filter simpleKeyword(FilterKeyword key, Object value) {
        filter.filter.put(fieldName, Map.of(key.getKeyword(), value));
        return filter;
    }

    /**
     * "fieldName": "value" ($eq is omitted)
     *
     * @param value
     *      value
     * @return
     *      self reference
     */
    public Filter isEqualsTo(Object value) {
        filter.filter.put(fieldName, value);
        return filter;
    }

    /**
     * $eq: [ ... ]
     *
     * @param value
     *      value
     * @return
     *      self reference
     */
    public Filter isAnArrayContaining(Object... value) {
        return simpleOperator(FilterOperator.EQUALS_TO, value);
    }

    /**
     * $all: [ ... ]
     *
     * @param value
     *      value
     * @return
     *      self reference
     */
    public Filter isAnArrayExactlyEqualsTo(Object[] value) {
        filter.filter.put(fieldName, Map.of(FilterKeyword.ALL.getKeyword(), value));
        return filter;
    }

    /**
     * $eq: [ ... ]
     *
     * @param value
     *      value
     * @return
     *      self reference
     */
    public Filter hasSubFieldsEqualsTo(Map<String, Object> value) {
        return simpleOperator(FilterOperator.EQUALS_TO, value);
    }

    /**
     * Add condition is less than.
     *
     * @param value
     *      value
     * @return
     *      self reference
     */
    public Filter isLessThan(Object value) {
        return simpleOperator(FilterOperator.LESS_THAN, value);
    }
    
    /**
     * Add condition is less than.
     *
     * @param value
     *      value
     * @return
     *      self reference
     */
    public Filter isLessOrEqualsThan(Object value) {
        return simpleOperator(FilterOperator.LESS_THAN_OR_EQUALS_TO, value);
    }
    
    /**
     * Add condition is less than.
     *
     * @param value
     *      value
     * @return
     *      self reference
     */        
    public Filter isGreaterThan(Object value) {
        return simpleOperator(FilterOperator.GREATER_THAN, value);
    }
    
    /**
     * Add condition is greater than.
     *
     * @param value
     *      value
     * @return
     *      self reference
     */        
    public Filter isGreaterOrEqualsThan(Object value) {
        return simpleOperator(FilterOperator.GREATER_THAN_OR_EQUALS_TO, value);
    }
    
    /**
     * Add condition is not equals to.
     *
     * @param value
     *      value
     * @return
     *      self reference
     */        
    public Filter isNotEqualsTo(Object value) {
        return simpleOperator(FilterOperator.NOT_EQUALS_TO, value);
    }
    
    /**
     * Add condition exists.
     *
     * @return
     *      self reference
     */
    public Filter exists() {
        return simpleKeyword(FilterKeyword.EXISTS, true);
    }
    
    /**
     * Condition to evaluate size
     *
     * @param size
     *      current size value
     * @return
     *      self reference
     */
    public Filter hasSize(int size) {
        return simpleKeyword(FilterKeyword.SIZE, true);
    }

}
