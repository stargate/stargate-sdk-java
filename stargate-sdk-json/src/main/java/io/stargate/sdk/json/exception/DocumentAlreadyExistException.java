package io.stargate.sdk.json.exception;

import io.stargate.sdk.json.domain.JsonApiError;

/**
 * Error Specialization.
 */
public class DocumentAlreadyExistException extends JsonApiException {

    /**
     * Constructor.
     *
     * @param error
     *      error message
     */
    public DocumentAlreadyExistException(JsonApiError error) {
        super(error);
    }

}
