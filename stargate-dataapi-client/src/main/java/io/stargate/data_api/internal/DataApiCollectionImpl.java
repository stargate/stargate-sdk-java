package io.stargate.data_api.internal;

import io.stargate.data_api.client.DataApiCollection;
import io.stargate.data_api.internal.model.ApiResponse;
import io.stargate.data_api.internal.model.InsertOneResult;
import io.stargate.sdk.http.ServiceHttp;
import io.stargate.sdk.utils.Assert;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import static io.stargate.data_api.internal.DataApiUtils.executeOperation;
import static io.stargate.sdk.utils.Assert.hasLength;
import static io.stargate.sdk.utils.Assert.notNull;

/**
 * Class representing a Data Api Collection.
 *
 * @param <DOC>
 *     working document
 */
@Slf4j
public class DataApiCollectionImpl<DOC> implements DataApiCollection<DOC> {

    /** Collection identifier. */
    @Getter
    private final String collectionName;

    /** Keep ref to the generic. */
    protected final Class<DOC> documentClass;

    /** keep reference to namespace client. */
    private final DataApiNamespaceImpl namespace;

    /** Resource collection. */
    public final Function<ServiceHttp, String> collectionResource;

    /**
     * Full constructor.
     *
     * @param namespaceClient
     *      client namespace http
     * @param collectionName
     *      collection identifier
     */
    protected DataApiCollectionImpl(DataApiNamespaceImpl namespaceClient,String collectionName, Class<DOC> clazz) {
        hasLength(collectionName, "collectionName");
        notNull(namespaceClient, "namespace client");
        this.collectionName  = collectionName;
        this.namespace       = namespaceClient;
        this.documentClass   = clazz;
        this.collectionResource = (node) -> namespaceClient.getNamespaceResource().apply(node) + "/" + collectionName;
    }

    // --------------------------
    // ---   Insert One      ----
    // --------------------------

    /**
     * Insert with a Json Document.
     *
     * @param document
     *     document to be inserted
     * @return
     *      document identifier and status
     */
    public final InsertOneResult insertOne(DOC document) {
        notNull(document, "document");
        ApiResponse apiResponse = execute("insertOne", Map.of("document", document));
        if (apiResponse.getErrors() != null && !apiResponse.getErrors().isEmpty()) {
            apiResponse.getErrors().get(0).throwDataApiException();
        }
        return new InsertOneResult(apiResponse.getStatus().get("insertedId"));
    }

    /**
     * Insert with a Json Document asynchronously
     *
     * @param document
     *     document to be inserted
     * @return
     *      document identifier and status
     */
    public final CompletableFuture<InsertOneResult> insertOneAsync(DOC document) {
        return CompletableFuture.supplyAsync(() -> insertOne(document));
    }

    /**
     * Syntax sugar.
     *
     * @param operation
     *      operation to run
     * @param payload
     *      payload returned
     */
    private ApiResponse execute(String operation, Object payload) {
        return executeOperation(namespace.getApiClient().getStargateHttpClient(), collectionResource, operation, payload);
    }

}
