package io.stargate.sdk.json;

import io.stargate.sdk.http.LoadBalancedHttpClient;
import io.stargate.sdk.http.ServiceHttp;
import io.stargate.sdk.json.domain.CollectionDefinition;
import io.stargate.sdk.json.domain.JsonApiResponse;
import io.stargate.sdk.utils.Assert;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;

import static io.stargate.sdk.json.utils.JsonApiClientUtils.executeOperation;
import static io.stargate.sdk.utils.AnsiUtils.green;

/**
 * Work with namespace and collections.
 */
@Slf4j
public class JsonNamespacesClient {

    /** Get Topology of the nodes. */
    protected final LoadBalancedHttpClient stargateHttpClient;

    /** Collection. */
    private String namespace;

    /**
     * /v1/{namespace}
     */
    public final Function<ServiceHttp, String> namespaceResource = (node) ->
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
    // ----     Collections operations       ----
    // ------------------------------------------

    /**
     * Find Collections.
     *
     * @return
     *       a list of Collections
     */
    public Stream<String> findCollections() {
        return execute("findCollections", null).getStatusKeyAsStream("collections");
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
        execute("createCollection", req);
        log.info("Collection  '" + green("{}") + "' has been created", req.getName());
    }

    /**
     * Drop a Collection, no error if it does snot exists.
     *
     * @param collection
     *      current Collection
     */
    public void dropCollection(String collection) {
        execute("dropCollection", Map.of("name", collection));
        log.info("Collection  '" + green("{}") + "' has been deleted", collection);
    }

    /**
     * Syntax sugar.
     *
     * @param operation
     *      operation to run
     * @param payload
     *      payload returned
     */
    private JsonApiResponse execute(String operation, Object payload) {
        return executeOperation(stargateHttpClient, namespaceResource, operation, payload);
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
