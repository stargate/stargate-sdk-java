package io.stargate.sdk.data.exception;

import io.stargate.sdk.data.domain.ApiError;

/**
 * Error Specialization.
 */
public class DataApiDocumentAlreadyExistException extends DataApiException {

    /**
     * Constructor.
     *
     * @param error
     *      error message
     */
    public DataApiDocumentAlreadyExistException(ApiError error) {
        super(error);
    }

}
