package io.stargate.sdk.data.client.exception;

import io.stargate.sdk.data.client.DataApiLimits;

/**
 * Error when too many documents in the collection
 */
public class TooManyDocumentsException extends Exception {

    /**
     * Default constructor.
     */
    public TooManyDocumentsException() {
        super("Collection has too many documents to count them all must be less then '" + DataApiLimits.MAX_DOCUMENTS_COUNT);
    }
}
