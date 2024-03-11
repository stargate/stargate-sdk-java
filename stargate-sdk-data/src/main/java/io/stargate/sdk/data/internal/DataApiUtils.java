package io.stargate.sdk.data.internal;

import io.stargate.sdk.data.client.exception.DataApiException;
import io.stargate.sdk.data.client.model.Document;
import io.stargate.sdk.data.internal.model.ApiData;
import io.stargate.sdk.data.internal.model.ApiError;
import io.stargate.sdk.data.internal.model.ApiResponse;
import io.stargate.sdk.http.LoadBalancedHttpClient;
import io.stargate.sdk.http.ServiceHttp;
import io.stargate.sdk.http.domain.ApiResponseHttp;
import io.stargate.sdk.utils.JsonUtils;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.util.function.Function;

import static io.stargate.sdk.utils.AnsiUtils.magenta;
import static io.stargate.sdk.utils.AnsiUtils.yellow;

/**
 * Utilities for the Json Client
 */
@Slf4j
public class DataApiUtils {

    /**
     * Hide Constructor.
     */
    private DataApiUtils() {}

    /**
     * Wrapper to execute Http POST request.
     *
     * @param stargateHttpClient
     *      http client
     * @param rootResource
     *      rest resource
     * @param jsonCommand
     *      json Command
     * @return
     *      json Api response
     */
    public static ApiResponse runCommand(
          @NonNull LoadBalancedHttpClient stargateHttpClient,
          @NonNull Function<ServiceHttp, String> rootResource,
          String jsonCommand) {
        log.debug(magenta("[request ]") + "=" + yellow("{}"), jsonCommand);
        ApiResponseHttp httpRes = stargateHttpClient.POST(rootResource, jsonCommand);
        log.debug(magenta("[response]") + "=" + yellow("{}"), httpRes.getBody());
        ApiResponse jsonRes = JsonUtils.unmarshallBeanForDataApi(httpRes.getBody(), ApiResponse.class);
        if (jsonRes.getData() != null) {
            ApiData data = jsonRes.getData();
            if (data.getDocument() != null) {
                log.debug("[apiData/document]=" + yellow("1 document retrieved, id='{}'"), data.getDocument().get(Document.ID));
            }
            if (data.getDocuments() != null) {
                log.debug("[apiData/documents]=" + yellow("{} document(s)."), data.getDocuments().size());
            }
        }

        // If insertedIds is present then it could lead to upsert
        if (jsonRes.getStatus()!= null && !jsonRes.getStatus().containsKey("insertedIds") ||
                (jsonRes.getStatus()==null && jsonRes.getErrors() != null)) {
            DataApiUtils.validate(jsonRes);
        }
        return jsonRes;
    }

    /**
     * Wrapper to execute Http POST request.
     *
     * @param stargateHttpClient
     *      http client
     * @param operation
     *      operation name
     * @param rootResource
     *      rest resource
     * @param body
     *      body to send
     * @return
     *      json Api response
     */
    public static ApiResponse runCommand(
            @NonNull LoadBalancedHttpClient stargateHttpClient,
            @NonNull Function<ServiceHttp, String> rootResource,
            @NonNull String operation, Object body) {
        String jsonCommand = buildJsonCommand(operation, body);
        return runCommand(stargateHttpClient, rootResource, jsonCommand);
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
    public static String buildJsonCommand(String function, Object content) {
        String stringBody = "{\"" + function + "\":";
        if (content == null) {
            stringBody += "{}";
        } else if (content instanceof String) {
            stringBody += (String) content;
        } else {
            String obj = JsonUtils.marshallForDataApi(content);
            stringBody += obj;
        }
        stringBody += "}";
        return stringBody;
    }

    /**
     * Body with no payload.
     *
     * @param function
     *      current function
     * @return
     *      request
     */
    public static String buildJsonCommand(String function) {
        return buildJsonCommand(function, null);
    }

    /**
     * Parse Errors in the output body if present.
     *
     * @param response returned by the Api.
     *     body to parse
     */
    public static void validate(@NonNull ApiResponse response) {
        if (response.getStatus() == null && response.getErrors() !=null && !response.getErrors().isEmpty()) {
            ApiError jsonApiError = response.getErrors().get(0);
            // Trace ERROR
            if (log.isDebugEnabled()) {
                log.debug("{} errors detected.", response.getErrors().size());
                log.debug("[ERROR]");
                log.debug("- message: {}", jsonApiError.getMessage());
                if (jsonApiError.getExceptionClass() != null) {
                    log.debug("- exceptionClass: {}", jsonApiError.getExceptionClass());
                }
                if (jsonApiError.getErrorCode() != null) {
                    log.debug("- errorCode: {}", jsonApiError.getErrorCode());
                }
            }
            jsonApiError.throwDataApiException();
        }

        if (response.getStatus() != null && response.getStatus().containsKey("ok") &&
            !response.getStatus().get("ok").equals(1)) {
            throw new DataApiException(response.getStatus().toString(), null, null);
        }
    }
}
