package io.stargate.sdk.json.exception;

import io.stargate.sdk.json.domain.JsonApiError;

/**
 * An error ocured with the JSON API
 */
public class JsonApiException extends RuntimeException {

    /** Serial. */
    private static final long serialVersionUID = 1L;

    /**
     * default error message.
     */
    public static final String DEFAULT_ERROR_MESSAGE = "Unexpected error at API Level.";

    /** Return the error. */
    private JsonApiError jsonApiError;

    /**
     * Error = exception.
     *
     * @param error
     *      error at api level
     */
    public JsonApiException(JsonApiError error) {
        super(error == null ? DEFAULT_ERROR_MESSAGE : error.getErrorMessage());
    }

    /**
     * Json Api Exception.
     *
     * @param error
     *      error message
     * @param cause
     *      error cause
     */
    public JsonApiException(JsonApiError error, Throwable cause) {
        super(error == null ? DEFAULT_ERROR_MESSAGE : error.getErrorMessage(), cause);
    }

    /**
     * Gets errorCode
     *
     * @return value of errorCode
     */
    public JsonApiError getJsonApiError() {
        return jsonApiError;
    }
}
