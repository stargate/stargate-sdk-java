package io.stargate.sdk.data.client.model;

/**
 * Provide helper to create a sort clause.
 */
public class Sorts {

    /**
     * Add a Sort ascending clause.
     *
     * @param fieldName
     *      current field name
     * @return
     *      target document.
     */
    public static Document asc(String fieldName) {
        return new Document().append(fieldName, SortOrder.ASCENDING.getOrder());
    }

    /**
     * Add a Sort descending clause.
     *
     * @param fieldName
     *      current field name
     * @return
     *      target document.
     */
    public static Document desc(String fieldName) {
        return new Document().append(fieldName, SortOrder.DESCENDING.getOrder());
    }

}
