package io.stargate.sdk.json.exception;

/**
 * An error occured with the JSON API
 */
public class JsonApiException extends RuntimeException {

    /** Serial. */
    private static final long serialVersionUID = 1L;

    public JsonApiException(String message) {
        super(message);
    }

    public JsonApiException(String message, String exceptionClass) {
        super(exceptionClass + ":" + message);
    }

    public JsonApiException(String message,  String exceptionClass, Throwable cause) {
        super(exceptionClass + ":" + message, cause);
    }

}
