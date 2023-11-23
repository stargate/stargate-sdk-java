package io.stargate.sdk.json.exception;

/**
 * An error ocured with the JSON API
 */
public class ApiException extends RuntimeException {

    /** Serial. */
    private static final long serialVersionUID = 1L;
    private String errorCode;

    /**
     * Default exception.
     *
     * @param message
     *      error message
     */
    public ApiException(String message) {
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
    public ApiException(String message, String exceptionClass) {
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
    public ApiException(String message, String exceptionClass, Throwable cause) {
        super(exceptionClass + ":" + message, cause);
    }

    public ApiException(String message, String exceptionClass, Throwable cause, String errorCode) {
        super(exceptionClass + ":" + message, cause);
        this.errorCode = errorCode;
    }

}
