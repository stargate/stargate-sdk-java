package io.stargate.data_api.internal.model;

import io.stargate.data_api.client.exception.DataApiException;
import lombok.Data;

/**
 * Subpart of the payload for json api response holding error information.
 */
@Data
public class ApiError {

    /**
     * Error message.
     */
    String message;

    /**
     * Error code.
     */
    String errorCode;

    /**
     * Error class.
     */
    String exceptionClass;

    /**
     * Default constructor.
     */
    public ApiError() {}

    /**
     * Build error message.
     *
     * @return
     *      error message
     */
    public String getErrorMessage() {
        StringBuilder sb = new StringBuilder();
        if (exceptionClass != null) {
            sb.append(exceptionClass).append(":");
        }
        if (errorCode != null) {
            sb.append(" (").append(errorCode).append(")");
        }
        if (message != null) {
            sb.append(message);
        }
        return sb.toString();
    }

    /**
     * Throw exception from an ApiError.
     */
    public void throwDataApiException() {
        throw new DataApiException(getMessage(), getErrorCode(), getErrorMessage());
    }
}
