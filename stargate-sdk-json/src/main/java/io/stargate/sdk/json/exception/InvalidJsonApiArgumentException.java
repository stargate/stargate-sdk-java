package io.stargate.sdk.json.exception;

import io.stargate.sdk.json.domain.JsonApiError;

/**
 * Error on Argument
 */
public class InvalidJsonApiArgumentException  extends JsonApiException {

    /**
     * Constructor.
     *
     * @param error
     *      error message
     */
    public InvalidJsonApiArgumentException(JsonApiError error) {
        super(error);
    }
}
