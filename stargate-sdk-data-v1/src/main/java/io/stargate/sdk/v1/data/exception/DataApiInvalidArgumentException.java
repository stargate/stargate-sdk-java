package io.stargate.sdk.v1.data.exception;

import io.stargate.sdk.v1.data.domain.ApiError;

/**
 * Error on Argument
 */
public class DataApiInvalidArgumentException extends DataApiException {

    /**
     * Constructor.
     *
     * @param error
     *      error message
     */
    public DataApiInvalidArgumentException(ApiError error) {
        super(error);
    }
}
