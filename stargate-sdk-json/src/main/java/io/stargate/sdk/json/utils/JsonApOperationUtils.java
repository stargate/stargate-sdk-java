package io.stargate.sdk.json.utils;

import io.stargate.sdk.json.exception.JsonApiException;
import io.stargate.sdk.utils.JsonUtils;

import java.util.Map;
import java.util.Objects;

public class JsonApOperationUtils {

    public static String buildRequestBody(String function, Object content) {
        return "{\"" +
                function +
                "\":" +
                (content == null ? "{}" : JsonUtils.marshall(content)) +
                "}";
    }

    public static String buildRequestBody(String function) {
        return buildRequestBody(function, null);
    }

    public static void handleErrors(Map<?,?> body) {
        Objects.requireNonNull(body, "body");
        if (body.containsKey("error")) {
            Map<?,?> error = (Map<?,?>) body.get("error");
            String message = "Error in creating the Keyspace";
            if (error.containsKey("message")) {
                message = (String) error.get("message");
            }
            String exceptionClass="";
            if (error.containsKey("exceptionClass")) {
                exceptionClass = (String) error.get("exceptionClass");
            }
            throw new JsonApiException(message, exceptionClass);
        }
    }
}
