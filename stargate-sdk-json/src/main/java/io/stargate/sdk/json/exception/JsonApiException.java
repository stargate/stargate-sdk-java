package io.stargate.sdk.json.exception;

/**
 * An error ocured with the JSON API
 */
public class JsonApiException extends RuntimeException {

    /** Serial. */
    private static final long serialVersionUID = 1L;

    /**
     * Default exception.
     *
     * @param message
     *      error message
     */
    public JsonApiException(String message) {
        super(message);
    }

    /**
     * Error = exception.
     *
     * @param message
     *      error message
     * @param exceptionClass
     *      error exception
     */
    public JsonApiException(String message, String exceptionClass) {
        super(exceptionClass + ":" + message);
    }

    /**
     * Json Api Exception.
     *
     * @param message
     *      error message
     * @param exceptionClass
     *      error exception
     * @param cause
     *      error cause
     */
    public JsonApiException(String message,  String exceptionClass, Throwable cause) {
        super(exceptionClass + ":" + message, cause);
    }

}
