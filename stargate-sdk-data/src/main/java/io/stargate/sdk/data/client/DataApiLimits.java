package io.stargate.sdk.data.client;

/**
 * Limits of the Data Api endpoint
 */
public interface DataApiLimits {

    /** Number of documents for a count. */
    int MAX_DOCUMENTS_COUNT = 1000;

    /** Maximum number of documents in a page. */
    int MAX_PAGE_SIZE = 20;

    /** Maximum number of documents when you insert. */
    int MAX_DOCUMENTS_IN_INSERT = 20;


}
