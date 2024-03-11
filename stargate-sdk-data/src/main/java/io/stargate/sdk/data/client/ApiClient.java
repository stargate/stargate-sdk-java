package io.stargate.sdk.data.client;

import io.stargate.sdk.data.internal.DataApiUtils;
import io.stargate.sdk.data.internal.model.ApiResponse;
import io.stargate.sdk.http.LoadBalancedHttpClient;
import io.stargate.sdk.http.ServiceHttp;
import io.stargate.sdk.utils.JsonUtils;

import java.util.function.Function;

/**
 * Use to initialize the HTTPClient.
 */
public interface ApiClient {

    /**
     * Lookup for an endpoint (Load balancing)
     *
     * @return
     *      endpoint in a distributed
     */
    Function<ServiceHttp, String> lookup();

    /**
     * Access the HTTP client singleton.
     *
     * @return
     *      endpoint in a distributed
     */
    LoadBalancedHttpClient getHttpClient();

    // ------------------------------------------
    // ----           Command                ----
    // ------------------------------------------

    /**
     * Command to return the payload as a Map.
     *
     * @param jsonCommand
     *     command as a json Payload
     * @return
     *     result as a document map
     */
    default ApiResponse runCommand(String jsonCommand) {
        return DataApiUtils.runCommand(getHttpClient(), lookup(), jsonCommand);
    }

    /**
     * Extension point to run any command with typing constraints.
     * @param jsonCommand
     *      command as a json Payload
     * @param documentClass
     *      document class to use for marshalling
     * @return
     *      instance of expecting type.
     * @param <DOC>
     *      document type to use
     */
    default <DOC> DOC runCommand(String jsonCommand, Class<DOC> documentClass) {
        ApiResponse api = runCommand(jsonCommand);
        String payload;
        if (api.getData() != null) {
            if (api.getData().getDocument() != null) {
                payload = JsonUtils.marshallForDataApi(api.getData().getDocument());
            } else if (api.getData().getDocuments() != null) {
                payload = JsonUtils.marshallForDataApi(api.getData().getDocuments());
            } else {
                throw new IllegalStateException("Cannot marshall into '" + documentClass + "' no documents returned.");
            }
        } else {
            payload = JsonUtils.marshallForDataApi(api.getStatus());
        }
        return JsonUtils.unmarshallBeanForDataApi(payload, documentClass);
    }
}
