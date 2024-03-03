package io.stargate.data_api.client.exception;

import io.stargate.data_api.internal.model.ApiError;
import lombok.Getter;

/**
 * An error occured with the JSON API
 */
@Getter
public class DataApiException extends RuntimeException {

    /** Serial. */
    private static final long serialVersionUID = 1L;

    /** Default error message. */
    public static final String DEFAULT_ERROR_MESSAGE = "Unexpected error for Data API";

    /** Default error code. */
    public static final String DEFAULT_ERROR_CODE = "CLIENT_ERROR";

    /** Default error message. */
    public static final String DEFAULT_EXCEPTION_CLASS = DataApiException.class.getName();

    /** Error. */
    private String errorCode = DEFAULT_ERROR_CODE;

    /** Exception message. */
    private String exceptionClass = DEFAULT_ERROR_MESSAGE;

    /**
     * Error = exception.
     *
     * @param msg
     *      error message
     * @param code
     *      error code
     * @param exceptionClass
     *      error at exception level
     */
    public DataApiException(String msg, String code, String exceptionClass) {
        super(msg);
        if (code != null) {
            this.errorCode = code;
        }
        if (exceptionClass != null) {
            this.exceptionClass = exceptionClass;
        }
    }

    /**
     * Error = exception.
     *
     * @param msg
     *      error message
     * @param code
     *      error code
     * @param exceptionClass
     *      error at exception level
     * @param parent
     *      parent exception
     */
    public DataApiException(String msg, String code, String exceptionClass, Throwable parent) {
        super(msg, parent);
        this.errorCode = code;
        this.exceptionClass = exceptionClass;
    }

    /**
     * Populate error.
     *
     * @param code
     *      error code
     * @param exceptionClass
     *      error class
     */
    private void populateError(String code, String exceptionClass) {
        if (code != null) {
            this.errorCode = code;
        }
        if (exceptionClass != null) {
            this.exceptionClass = exceptionClass;
        }
    }

}
