package io.stargate.sdk.json.utils;

import io.stargate.sdk.http.LoadBalancedHttpClient;
import io.stargate.sdk.http.ServiceHttp;
import io.stargate.sdk.http.domain.ApiResponseHttp;
import io.stargate.sdk.json.domain.JsonApiError;
import io.stargate.sdk.json.domain.JsonApiResponse;
import io.stargate.sdk.json.exception.JsonApiException;
import io.stargate.sdk.utils.JsonUtils;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

import static io.stargate.sdk.utils.AnsiUtils.yellow;

/**
 * Utilities for the Json Client
 */
@Slf4j
public class JsonApiClientUtils {

    /**
     * Hide Constructor.
     */
    private JsonApiClientUtils() {}

    /**
     * Accessing api.
     * @param operation
     *      expected operation
     * @param body
     *      body to send
     * @return
     *      json Api response
     */
    public static JsonApiResponse executeOperation(
            @NonNull LoadBalancedHttpClient stargateHttpClient,
            @NonNull Function<ServiceHttp, String> rootResource,
            @NonNull String operation, Object body) {
        String stringBody = "{\"" + operation + "\":" + (body == null ? "{}" : JsonUtils.marshall(body)) + "}";
        log.debug(operation + "[request]=" + yellow("{}"), stringBody);
        ApiResponseHttp httpRes = stargateHttpClient.POST(rootResource, stringBody);
        log.debug(operation + "[response]=" + yellow("{}"), httpRes.getBody());
        JsonApiResponse jsonRes = JsonUtils.unmarshallBean(httpRes.getBody(), JsonApiResponse.class);
        JsonApiClientUtils.validate(jsonRes);
        return jsonRes;
    }

    /**
     * Build request Body as expect by the api
     * { 'operation': { 'content': 'content' } }
     *
     * @param function
     *      the function to call
     * @param content
     *      Object to serialize as json
     */
    public static String buildRequestBody(String function, Object content) {
        return "{\"" +
                function +
                "\":" +
                (content == null ? "{}" : JsonUtils.marshall(content)) +
                "}";
    }

    /**
     * Body with no payload.
     *
     * @param function
     *      current function
     * @return
     *      request
     */
    public static String buildRequestBody(String function) {
        return buildRequestBody(function, null);
    }

    /**
     * Parse Errors in the output body if present.
     * @param body
     *     body to parse
     */
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

    /**
     * Parse Errors in the output body if present.
     *
     * @param response returned by the Api.
     *     body to parse
     */
    public static void validate(@NonNull JsonApiResponse response) {
        if (response.getErrors() !=null && !response.getErrors().isEmpty()) {
            log.error("{} errors detected.", response.getErrors().size());
            for (JsonApiError error : response.getErrors()) {
                log.error("[ERROR]");
                log.error("- message: {}", error.getMessage());
                log.error("- exceptionClass: {}", error.getExceptionClass());
                if (error.getErrorCode() !=null) {
                    log.error("-  errorCode: {}", error.getErrorCode());
                }
            }
            throw new JsonApiException(
                    response.getErrors().get(0).getMessage(),
                    response.getErrors().get(0).getExceptionClass());
        }
        if (response.getStatus() != null &&
            response.getStatus().containsKey("ok") &&
            !response.getStatus().get("ok").equals(1)) {
            throw new JsonApiException("Operation failed: " + response.getStatus());
        }
    }
}
