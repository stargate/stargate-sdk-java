package io.stargate.sdk.json;

import io.stargate.sdk.http.LoadBalancedHttpClient;
import io.stargate.sdk.http.ServiceHttp;
import io.stargate.sdk.http.domain.ApiResponseHttp;
import io.stargate.sdk.json.domain.CollectionDefinition;
import io.stargate.sdk.json.exception.JsonApiException;
import io.stargate.sdk.json.utils.JsonApOperationUtils;
import io.stargate.sdk.utils.Assert;
import io.stargate.sdk.utils.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;

import static io.stargate.sdk.utils.AnsiUtils.green;

/**
 * Work with namespace and collections.
 */
public class JsonNamespacesClient {

    /** Logger for our Client. */
    private static final Logger LOGGER = LoggerFactory.getLogger(JsonNamespacesClient.class);

    /** Get Topology of the nodes. */
    protected final LoadBalancedHttpClient stargateHttpClient;

    /** Collection. */
    private String namespace;

    /**
     * /v1/{namespace}
     */
    public Function<ServiceHttp, String> namespaceResource = (node) ->
            StargateJsonApiClient.rootResource.apply(node) + "/" + namespace;

    /**
     * Full constructor.
     *
     * @param httpClient client http
     * @param namespace namespace identifier
     */
    public JsonNamespacesClient(LoadBalancedHttpClient httpClient, String namespace) {
        this.namespace          = namespace;
        this.stargateHttpClient = httpClient;
        Assert.notNull(namespace, "namespace");
    }

    // ------------------------------------------
    // ----     Collections operations        ----
    // ------------------------------------------

    /**
     * Find Collections.
     *
     * @return
     *       a list of Collections
     */
    @SuppressWarnings("unchecked")
    public Stream<String> findCollections() {
        ApiResponseHttp res = stargateHttpClient.POST(namespaceResource, "{\"findCollections\": {}}");
        Map<?,?> body = JsonUtils.unmarshallBean(res.getBody(), Map.class);
        if (body.containsKey("status")) {
            Map<?,?> status = (Map<?,?>) body.get("status");
            if (status.containsKey("collections")) {
                return ((ArrayList<String>) status.get("collections")).stream();
            }
        }
        return Stream.empty();
    }

    /**
     * Create a Collection providing a name.
     *
     * @param collection
     *      current Collection.
     */
    public void createCollection(String collection) {
        this.createCollection(CollectionDefinition.builder().name(collection).build());
    }

    /**
     * Create a Collection providing a name.
     *
     * @param req
     *      current Collection.
     */
    public void createCollection(CollectionDefinition req) {
        String stringBody = JsonApOperationUtils.buildRequestBody("createCollection", req);
        ApiResponseHttp res = stargateHttpClient.POST(namespaceResource, stringBody);
        Map<?,?> body = JsonUtils.unmarshallBean(res.getBody(), Map.class);
        JsonApOperationUtils.handleErrors(body);
        if (body.containsKey("status")) {
            Map<?,?> status = (Map<?,?>) body.get("status");
            if (status.containsKey("ok")) {
                LOGGER.info("Collection  '" + green("{}") + "' has been created", req.getName());
            }
        } else {
            throw new JsonApiException("Cannot create Collection: " + body);
        }
    }

    /**
     * Drop a Collection, no error if it does snot exists.
     *
     * @param collection
     *      current Collection
     */
    public void dropCollection(String collection) {
        String stringBody = JsonApOperationUtils.buildRequestBody("deleteCollection",
                JsonUtils.marshall(Map.of("name", collection)));
        ApiResponseHttp res = stargateHttpClient.POST(JsonNamespacesClient.this.namespaceResource, stringBody);
        Map<?,?> body = JsonUtils.unmarshallBean(res.getBody(), Map.class);
        JsonApOperationUtils.handleErrors(body);
        if (res.getCode() != 200) {
            throw new JsonApiException("Cannot drop Collection: " + res.getBody());
        }
    }

    // ---------------------------------
    // ----    Sub Resources        ----
    // ---------------------------------

    /**
     * Move the document API (namespace client).
     *
     * @param collection
     *      collection name
     * @return JsonDocumentsClient
     *      client to work with documents
     */
    public JsonDocumentsClient collection(String collection) {
        return new JsonDocumentsClient(stargateHttpClient, namespace, collection);
    }

}
