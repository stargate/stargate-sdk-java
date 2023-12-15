package io.stargate.sdk.json.domain;

import lombok.Data;

/**
 * Payload for json Api error.
 */
@Data
public class JsonApiError {

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
    public JsonApiError() {}

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
}
