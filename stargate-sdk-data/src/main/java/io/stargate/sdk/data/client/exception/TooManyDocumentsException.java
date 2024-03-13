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
        super("Document count exceeds '" + DataApiLimits.MAX_DOCUMENTS_COUNT + ", the maximum allowed by the server");
    }

    /**
     * Default constructor.
     */
    public TooManyDocumentsException(int upperLimit) {
        super("Document count exceeds upper bound set in method call " + upperLimit);
    }
}
