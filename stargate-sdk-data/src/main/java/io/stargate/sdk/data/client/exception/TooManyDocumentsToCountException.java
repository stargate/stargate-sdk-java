package io.stargate.sdk.data.client.exception;

import io.stargate.sdk.data.client.DataApiClient;

/**
 * Error when too many documents in the collection
 */
public class TooManyDocumentsToCountException extends Exception {

    /**
     * Default constructor.
     */
    public TooManyDocumentsToCountException() {
        super("Document count exceeds '" + DataApiClient.MAX_DOCUMENTS_COUNT + ", the maximum allowed by the server");
    }

    /**
     * Default constructor.
     */
    public TooManyDocumentsToCountException(int upperLimit) {
        super("Document count exceeds upper bound set in method call " + upperLimit);
    }
}
