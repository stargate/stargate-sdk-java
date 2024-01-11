package io.stargate.sdk.data.exception;

import io.stargate.sdk.data.domain.ApiError;

/**
 * An error ocured with the JSON API
 */
public class DataApiException extends RuntimeException {

    /** Serial. */
    private static final long serialVersionUID = 1L;

    /**
     * default error message.
     */
    public static final String DEFAULT_ERROR_MESSAGE = "Unexpected error at API Level.";

    /** Return the error. */
    private final ApiError jsonApiError;

    /**
     * Error = exception.
     *
     * @param error
     *      error at api level
     */
    public DataApiException(ApiError error) {
        super(error == null ? DEFAULT_ERROR_MESSAGE : error.getErrorMessage());
        this.jsonApiError = error;
    }

    /**
     * Json Api Exception.
     *
     * @param error
     *      error message
     * @param cause
     *      error cause
     */
    public DataApiException(ApiError error, Throwable cause) {
        super(error == null ? DEFAULT_ERROR_MESSAGE : error.getErrorMessage(), cause);
        this.jsonApiError = error;
    }

    /**
     * Gets errorCode
     *
     * @return value of errorCode
     */
    public ApiError getJsonApiError() {
        return jsonApiError;
    }
}
