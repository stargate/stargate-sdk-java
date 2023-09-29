package io.stargate.sdk.doc.domain;

import io.stargate.sdk.http.domain.Filter;
import io.stargate.sdk.http.domain.FilterOperator;

import java.util.Collection;

/**
 * Helper to build a where clause in natural language (fluent API).
 * 
 * @author Cedrick LUNVEN (@clunven)
 */
public class QueryBuilderFilter {
    
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
    protected QueryBuilderFilter(QueryBuilder builder, String fieldName) {
        this.builder   = builder;
        this.fieldName = fieldName;
    }
    
    /**
     * Add a filter
     * @param op
     *      operation
     * @param value
     *      value
     * @return
     *      self reference
     */
    private QueryBuilder addFilter(FilterOperator op, Object value) {
        builder.filters.add(new Filter(fieldName,op, value));
        return builder;
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
        return addFilter(FilterOperator.LESS_THAN, value);
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
        return addFilter(FilterOperator.LESS_THAN_OR_EQUALS_TO, value);
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
        return addFilter(FilterOperator.GREATER_THAN, value);
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
        return addFilter(FilterOperator.GREATER_THAN_OR_EQUALS_TO, value);
    }
    
    /**
     * Add condition is is equals to.
     *
     * @param value
     *      value
     * @return
     *      self reference
     */        
    public QueryBuilder isEqualsTo(Object value) {
        return addFilter(FilterOperator.EQUALS_TO, value);
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
        return addFilter(FilterOperator.NOT_EQUALS_TO, value);
    }
    
    /**
     * Add condition exists.
     *
     * @return
     *      self reference
     */
    public QueryBuilder exists() {
        return addFilter(FilterOperator.EXISTS, null);
    }
    
    /**
     * Add condition is isIn.
     *
     * @param values
     *      values
     * @return
     *      self reference
     */
    public QueryBuilder isIn(Collection<Object> values) {
        return addFilter(FilterOperator.IN, values);
    }

}
