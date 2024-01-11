package io.stargate.sdk.data.exception;

import io.stargate.sdk.data.domain.ApiError;

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
