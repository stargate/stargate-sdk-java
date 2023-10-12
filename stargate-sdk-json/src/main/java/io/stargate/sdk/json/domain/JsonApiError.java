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
    public JsonApiError() {
    }
}
