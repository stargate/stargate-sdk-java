package io.stargate.sdk.data.client.model;

import io.stargate.sdk.http.domain.FilterOperator;

import java.util.Arrays;

/**
 * Helper to create Filter
 */
public class Filters {

    /**
     * Creates a filter that matches all documents where the value of _id field equals the specified value. Note that this doesn't
     * actually generate a $eq operator, as the query language doesn't require it.
     *
     * @param value
     *      the value, which may be null
     * @return
     *      the filter
     */
    public static Filter eq(final Object value) {
        return eq("_id", value);
    }

    /**
     * Help Building the filters.
     *
     * @param fieldName
     *      current fieldName
     * @param value
     *      current fieldValue
     * @return
     *      filter
     */
    public static Filter eq(String fieldName, Object value) {
        return new Filter().where(fieldName).isEqualsTo(value);
    }

    /**
     * Creates a filter that matches all documents where the value of the field name does not equal the specified value.
     *
     * @param fieldName
     *      the field name
     * @param value
     *      the value, which may be null
     * @return the filter
     */
    public static Filter ne(final String fieldName, final Object value) {
        return new Filter().where(fieldName).isNotEqualsTo(value);
    }

    /**
     * Creates a filter that matches all documents where the value of the given field is greater than the specified value.
     *
     * @param fieldName
     *      the field name
     * @param value
     *      the value, which may be null
     *
     * @return the filter
     */
    public static Filter gt(final String fieldName, final Number value) {
        return new Filter().where(fieldName).isGreaterThan(value);
    }

    /**
     * Creates a filter that matches all documents where the value of the given field is greater than or equal to the specified value.
     *
     * @param fieldName
     *      the field name
     * @param value
     *      the value, which may be null
     *
     * @return the filter
     */
    public static Filter gte(final String fieldName, final Number value) {
        return new Filter().where(fieldName).isGreaterOrEqualsThan(value);
    }

    /**
     * Creates a filter that matches all documents where the value of the given field is less than the specified value.
     *
     * @param fieldName
     *      the field name
     * @param value
     *      the value, which may be null
     *
     * @return the filter
     */
    public static Filter lt(final String fieldName, final Number value) {
        return new Filter().where(fieldName).isLessThan(value);
    }

    /**
     * Creates a filter that matches all documents where the value of the given field is less than or equal to the specified value.
     *
     * @param fieldName
     *      the field name
     * @param value
     *      the value, which may be null
     *
     * @return the filter
     */
    public static Filter lte(final String fieldName, final Number value) {
        return new Filter().where(fieldName, FilterOperator.LESS_THAN_OR_EQUALS_TO, value);
    }

    /**
     * Build a filter with the `$hasSize` operator.
     *
     * @param fieldName
     *      target field
     * @param size
     *      value for size (positive integer)
     * @return
     *      filter built
     */
    public static Filter hasSize(final String fieldName, final int size) {
        return new Filter().where(fieldName).hasSize(size);
    }

    /**
     * Build a filter with the `$exists` operator.
     *
     * @param fieldName
     *      target field
     * @return
     *      filter built
     */
    public static Filter exists(final String fieldName) {
        return new Filter().where(fieldName).exists();
    }

    /**
     * Build a filter with the `$all` operator.
     *
     * @param fieldName
     *      target field
     * @param values
     *     list of values for the condition
     * @return
     *      filter built
     */
    public static Filter all(final String fieldName, final Object... values) {
        return new Filter().where(fieldName).isAnArrayExactlyEqualsTo(values);
    }

    /**
     * Creates a filter that matches all documents where the value of a field equals any value in the list of specified values.
     *
     * @param fieldName
     *      the field name
     * @param values
     *      the list of values
     * @return the filter
     */
    @SafeVarargs
    @SuppressWarnings("varargs")
    public static <VAL> Filter in(final String fieldName, final VAL... values) {
        return new Filter().where(fieldName).isInArray(values);
    }

    /**
     * Creates a filter that matches all documents where the value of a field equals any value in the list of specified values.
     *
     * @param fieldName
     *      the field name
     * @param values
     *      the list of values
     * @return the filter
     */
    @SafeVarargs
    @SuppressWarnings("varargs")
    public static <VAL> Filter nin(final String fieldName, final VAL... values) {
        return new Filter().where(fieldName).isNotInArray(values);
    }

    /**
     * Creates a filter that performs a logical AND of the provided list of filters.
     *
     * <blockquote><pre>
     *    and(eq("x", 1), lt("y", 3))
     * </pre></blockquote>
     *
     * will generate a MongoDB query like:
     * <blockquote><pre>
     *    { $and: [{x : 1}, {y : {$lt : 3}}]}
     * </pre></blockquote>
     *
     * @param filters the list of filters to and together
     * @return the filter
     */
    public static Filter and(final Filter... filters) {
        return and(Arrays.asList(filters));
    }

    /**
     * Creates a filter that performs a logical AND of the provided list of filters.
     *
     * <blockquote><pre>
     *    and(eq("x", 1), lt("y", 3))
     * </pre></blockquote>
     *
     * will generate a MongoDB query like:
     * <blockquote><pre>
     *    { $and: [{x : 1}, {y : {$lt : 3}}]}
     * </pre></blockquote>
     *
     * @param filters the list of filters to and together
     * @return the filter
     */
    public static Filter and(final Iterable<Filter> filters) {
        Filter andFilter = new Filter();
        andFilter.documentMap.put("$and", filters);
        return andFilter;
    }

    /**
     * Creates a filter that performs a logical OR of the provided list of filters.
     *
     * <blockquote><pre>
     *    or(eq("x", 1), lt("y", 3))
     * </pre></blockquote>
     *
     * will generate a query like:
     * <blockquote><pre>
     *    { $or: [{x : 1}, {y : {$lt : 3}}]}
     * </pre></blockquote>
     *
     * @param filters the list of filters to and together
     * @return the filter
     */
    public static Filter or(final Iterable<Filter> filters) {
        Filter andFilter = new Filter();
        andFilter.documentMap.put("$or", filters);
        return andFilter;
    }

    /**
     * Creates a filter that performs a logical OR of the provided list of filters.
     *
     * <blockquote><pre>
     *    or(eq("x", 1), lt("y", 3))
     * </pre></blockquote>
     *
     * will generate a query like:
     * <blockquote><pre>
     *    { $or: [{x : 1}, {y : {$lt : 3}}]}
     * </pre></blockquote>
     *
     * @param filters the list of filters to and together
     * @return the filter
     */
    public static Filter or(final Filter... filters) {
        return or(Arrays.asList(filters));
    }

    /**
     * Creates a filter that performs a logical NOT of the provided filter
     *
     * <blockquote><pre>
     *    not(eq("x", 1))
     * </pre></blockquote>
     *
     * will generate a query like:
     * <blockquote><pre>
     *    { $and: [{x : 1}, {y : {$lt : 3}}]}
     * </pre></blockquote>
     *
     * @param filter the list of filters to and together
     * @return the filter
     */
    public static Filter not(Filter filter) {
        Filter andFilter = new Filter();
        andFilter.documentMap.put("$not", filter);
        return andFilter;
    }

}
