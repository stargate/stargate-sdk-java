package io.stargate.sdk.json.domain;

import io.stargate.sdk.http.domain.FilterKeyword;
import io.stargate.sdk.http.domain.FilterOperator;

import java.util.Map;

/**
 * Helper to build a where clause in natural language (fluent API).
 * 
 * @author Cedrick LUNVEN (@clunven)
 */
public class DeleteQueryFilterBuilder {

    /** Required field name. */
    private final String fieldName;

    /** Working builder to override the 'where' field and move with builder. */
    private final DeleteQueryBuilder builder;

    /**
     * Only constructor allowed
     *
     * @param builder SearchDocumentQueryBuilder
     * @param fieldName String
     */
    protected DeleteQueryFilterBuilder(DeleteQueryBuilder builder, String fieldName) {
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
    private DeleteQueryBuilder simpleOperator(FilterOperator cond, Object value) {
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
    private DeleteQueryBuilder simpleKeyword(FilterKeyword key, Object value) {
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
    public DeleteQueryBuilder isEqualsTo(Object value) {
        if (FilterKeyword.VECTOR.getKeyword().equals(fieldName)) {
            if (value instanceof float[]) {
                // As a vector it will be an ann
                builder.orderByAnn((float[])value);
            } else {
                throw new IllegalArgumentException("Vector must be an array of float");
            }
        } else {
            builder.filter.put(fieldName, value);
        }
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
    public DeleteQueryBuilder isAnArrayContaining(Object[] value) {
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
    public DeleteQueryBuilder isAnArrayExactlyEqualsTo(Object[] value) {
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
    public DeleteQueryBuilder hasSubFieldsEqualsTo(Map<String, Object> value) {
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
    public DeleteQueryBuilder isLessThan(Object value) {
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
    public DeleteQueryBuilder isLessOrEqualsThan(Object value) {
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
    public DeleteQueryBuilder isGreaterThan(Object value) {
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
    public DeleteQueryBuilder isGreaterOrEqualsThan(Object value) {
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
    public DeleteQueryBuilder isNotEqualsTo(Object value) {
        return simpleOperator(FilterOperator.NOT_EQUALS_TO, value);
    }
    
    /**
     * Add condition exists.
     *
     * @return
     *      self reference
     */
    public DeleteQueryBuilder exists() {
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
    public DeleteQueryBuilder hasSize(int size) {
        return simpleKeyword(FilterKeyword.SIZE, true);
    }

}
