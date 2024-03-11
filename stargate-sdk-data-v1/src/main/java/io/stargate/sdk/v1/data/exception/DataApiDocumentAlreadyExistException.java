package io.stargate.sdk.v1.data.exception;

import io.stargate.sdk.v1.data.domain.ApiError;

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
