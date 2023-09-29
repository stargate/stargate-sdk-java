package io.stargate.sdk.json.domain;

import io.stargate.sdk.http.domain.FilterOperator;
import io.stargate.sdk.http.domain.FilterKeyword;

import java.util.Collection;
import java.util.Map;

/**
 * Helper to build a where clause in natural language (fluent API).
 * 
 * @author Cedrick LUNVEN (@clunven)
 */
public class QueryFilterBuilder {
    
    /** Required field name. */
    private final String fieldName;
    
    /** Working builder to override the 'where' field and move with builder. */
    private final QueryBuilder builder;
    
    /**
     * Only constructor allowed
     * 
     * @param builder SearchDocumentQueryBuilder
     * @param fieldName String
     */
    protected QueryFilterBuilder(QueryBuilder builder, String fieldName) {
        this.builder   = builder;
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
    private QueryBuilder simpleOperator(FilterOperator cond, Object value) {
        builder.filter.put(fieldName, Map.of(cond.getOperator(), value));
        return builder;
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
    private QueryBuilder simpleKeyword(FilterKeyword key, Object value) {
        builder.filter.put(fieldName, Map.of(key.getKeyword(), value));
        return builder;
    }

    /**
     * "fieldName": "value" ($eq is omitted)
     *
     * @param value
     *      value
     * @return
     *      self reference
     */
    public QueryBuilder isEqualsTo(Object value) {
        builder.filter.put(fieldName, value);
        return builder;
    }

    /**
     * $eq: [ ... ]
     *
     * @param value
     *      value
     * @return
     *      self reference
     */
    public QueryBuilder isAnArrayContaining(Object[] value) {
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
    public QueryBuilder isAnArrayExactlyEqualsTo(Object[] value) {
        builder.filter.put(fieldName, Map.of(FilterKeyword.ALL.getKeyword(), value));
        return builder;
    }

    /**
     * $eq: [ ... ]
     *
     * @param value
     *      value
     * @return
     *      self reference
     */
    public QueryBuilder hasSubFieldsEqualsTo(Map<String, Object> value) {
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
    public QueryBuilder isLessThan(Object value) {
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
    public QueryBuilder isLessOrEqualsThan(Object value) {
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
    public QueryBuilder isGreaterThan(Object value) {
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
    public QueryBuilder isGreaterOrEqualsThan(Object value) {
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
    public QueryBuilder isNotEqualsTo(Object value) {
        return simpleOperator(FilterOperator.NOT_EQUALS_TO, value);
    }
    
    /**
     * Add condition exists.
     *
     * @return
     *      self reference
     */
    public QueryBuilder exists() {
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
    public QueryBuilder hasSize(int size) {
        return simpleKeyword(FilterKeyword.SIZE, true);
    }

}
