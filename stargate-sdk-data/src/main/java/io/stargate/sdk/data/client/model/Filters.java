package io.stargate.sdk.data.client.model;

import io.stargate.sdk.http.domain.FilterOperator;

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
    public static Filter and(final Iterable<Filter> filters) {
        return null;
        /*
        FilterBuilderList builderList = new Filter().and();
        filters.forEach(builderList.where())
                .where("product_price", FilterOperator.EQUALS_TO,12.99)
                .where("product_name", FilterOperator.EQUALS_TO, "HealthyFresh - Beef raw dog food")
                .end();
                */
    }



}
