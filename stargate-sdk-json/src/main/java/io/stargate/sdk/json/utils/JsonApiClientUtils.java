package io.stargate.sdk.json.utils;

import io.stargate.sdk.http.LoadBalancedHttpClient;
import io.stargate.sdk.http.ServiceHttp;
import io.stargate.sdk.http.domain.ApiResponseHttp;
import io.stargate.sdk.json.domain.JsonApiError;
import io.stargate.sdk.json.domain.JsonApiResponse;
import io.stargate.sdk.json.exception.ApiErrorCode;
import io.stargate.sdk.json.exception.JsonApiException;
import io.stargate.sdk.json.exception.DocumentAlreadyExistException;
import io.stargate.sdk.json.exception.InvalidJsonApiArgumentException;
import io.stargate.sdk.utils.JsonUtils;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

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
     *
     * @param stargateHttpClient
     *      http client
     * @param operation
     *      name of the operation
     * @param rootResource
     *      rest resource
     * @param body
     *      body to send
     * @return
     *      json Api response
     */
    public static JsonApiResponse executeOperation(
            @NonNull LoadBalancedHttpClient stargateHttpClient,
            @NonNull Function<ServiceHttp, String> rootResource,
            @NonNull String operation, Object body) {
        String stringBody = "{\"" + operation + "\":";
        if (body == null) {
            stringBody += "{}";
        } else if (body instanceof String) {
            stringBody += (String) body;
        } else {
            stringBody += JsonUtils.marshall(body);
        }
        stringBody += "}";
        log.debug(operation + "[request]=" + yellow("{}"), stringBody);
        ApiResponseHttp httpRes = stargateHttpClient.POST(rootResource, stringBody);
        log.debug(operation + "[response]=" + yellow("{}"), httpRes.getBody());
        JsonApiResponse jsonRes = JsonUtils.unmarshallBean(httpRes.getBody(), JsonApiResponse.class);
        if (jsonRes.getData() != null && jsonRes.getData().getDocuments() != null) {
            log.debug(operation + "[response]=" + yellow("{}"), jsonRes.getData().getDocuments());
        }
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
     * @return
     *    request body
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
     *
     * @param response returned by the Api.
     *     body to parse
     */
    public static void validate(@NonNull JsonApiResponse response) {
        if (response.getErrors() !=null && !response.getErrors().isEmpty()) {
            if (log.isDebugEnabled()) {
                log.debug("{} errors detected.", response.getErrors().size());
                for (JsonApiError error : response.getErrors()) {
                    log.debug("[ERROR]");
                    log.debug("- message: {}", error.getMessage());
                    log.debug("- exceptionClass: {}", error.getExceptionClass());
                    if (error.getErrorCode() != null) {
                        log.debug("- errorCode: {}", error.getErrorCode());
                    }
                }
            }
            JsonApiError jsonApiError = response.getErrors().get(0);
            try {
                // Specializing error from the code
                if (jsonApiError.getErrorCode() != null) {
                    final ApiErrorCode errorCode = ApiErrorCode.valueOf(jsonApiError.getErrorCode());
                    switch (errorCode) {
                        case DOCUMENT_ALREADY_EXISTS:
                            throw new DocumentAlreadyExistException(jsonApiError);
                        case INVALID_COLLECTION_NAME:
                        case INVALID_FILTER_EXPRESSION:
                            throw new InvalidJsonApiArgumentException(jsonApiError);
                    }
                }
                if (jsonApiError.getExceptionClass() != null) {
                    if ("ConstraintViolationException".equals(jsonApiError.getExceptionClass())) {
                        throw new InvalidJsonApiArgumentException(jsonApiError);
                    }
                }
            } catch(Exception e) {
                throw new JsonApiException(jsonApiError, e);
            }
            throw new JsonApiException(jsonApiError);
        }
        if (response.getStatus() != null &&
            response.getStatus().containsKey("ok") &&
            !response.getStatus().get("ok").equals(1)) {
            JsonApiError error = new JsonApiError();
            error.setErrorCode(JsonApiException.DEFAULT_ERROR_MESSAGE);
            error.setMessage(response.getStatus().toString());
            throw new JsonApiException(error);
        }
    }
}
