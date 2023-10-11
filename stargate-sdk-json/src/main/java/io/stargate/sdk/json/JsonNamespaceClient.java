package io.stargate.sdk.json;

import io.stargate.sdk.core.domain.ObjectMap;
import io.stargate.sdk.http.LoadBalancedHttpClient;
import io.stargate.sdk.http.ServiceHttp;
import io.stargate.sdk.json.domain.CollectionDefinition;
import io.stargate.sdk.json.domain.JsonApiResponse;
import io.stargate.sdk.json.vector.SimilarityMetric;
import io.stargate.sdk.json.exception.CollectionNotFoundException;
import io.stargate.sdk.json.vector.VectorStore;
import io.stargate.sdk.utils.Assert;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;

import static io.stargate.sdk.json.utils.JsonApiClientUtils.executeOperation;
import static io.stargate.sdk.utils.AnsiUtils.green;

/**
 * Work with namespace and collections.
 */
@Getter
@Slf4j
public class JsonNamespaceClient {

    /** Get Topology of the nodes. */
    protected final LoadBalancedHttpClient stargateHttpClient;

    /** Collection. */
    private String namespace;

    /**
     * /v1/{namespace}
     */
    public final Function<ServiceHttp, String> namespaceResource = (node) ->
            JsonApiClient.rootResource.apply(node) + "/" + namespace;

    /**
     * Full constructor.
     *
     * @param httpClient client http
     * @param namespace namespace identifier
     */
    public JsonNamespaceClient(LoadBalancedHttpClient httpClient, String namespace) {
        this.namespace          = namespace;
        this.stargateHttpClient = httpClient;
        Assert.notNull(namespace, "namespace");
    }

    // ------------------------------------------
    // ----     Collections operations       ----
    // ------------------------------------------

    /**
     * Evaluate if a collection exists.
     *
     * @param collection
     *      collection name.
     * @return
     *      if collection exists
     */
    public boolean existCollection(String collection) {
        return findCollections().anyMatch(collection::equals);
    }

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
     * Create a Collection for vector purpose
     *
     * @param collection name
     *      current Collection.
     * @param dimension
     *      dimension of the vector
     */
    public void createCollection(String collection, int dimension) {
        this.createCollection(CollectionDefinition.builder()
                .name(collection)
                .vector(dimension,
                        SimilarityMetric.cosine).build());
    }

    /**
     * Create a Collection for vector purpose
     *
     * @param collection name
     *      current Collection.
     * @param dimension
     *      dimension of the vector
     * @param metric
     *      similarity metric
     */
    public void createCollection(String collection, int dimension, SimilarityMetric metric) {
        this.createCollection(CollectionDefinition.builder()
                .name(collection)
                .vector(dimension, metric).build());
    }

    /**
     * Create a Collection for vector purpose
     *
     * @param collection name
     *      current Collection.
     * @param dimension
     *      dimension of the vector
     * @param metric
     *      similarity metric
     * @param llm
     *      llm service
     * @param model
     *      model to be used
     */
    public void createCollection(String collection, int dimension, SimilarityMetric metric, String llm, String model) {
        this.createCollection(CollectionDefinition.builder()
                .name(collection)
                .vector(dimension, metric)
                .vectorize(llm, model)
                .build());
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
     * Drop a Collection, no error if it does not exist.
     *
     * @param collection
     *      current Collection
     */
    public void deleteCollection(String collection) {
        execute("deleteCollection", Map.of("name", collection));
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
     * @param collectionName
     *      collection name
     * @return JsonDocumentsClient
     *      client to work with documents
     */
    public JsonCollectionClient collection(String collectionName) {
        if (findCollections().noneMatch(collectionName::equals)) throw new CollectionNotFoundException(collectionName);
        return new JsonCollectionClient(stargateHttpClient, namespace, collectionName);
    }

    public CollectionRepository<ObjectMap> repository(String collectionName) {
        return repository(collectionName, ObjectMap.class);
    }

    public <T> CollectionRepository<T> repository(String collectionName, Class<T> clazz) {
        if (!existCollection(collectionName)) throw new CollectionNotFoundException(collectionName);
        return new CollectionRepository<>(collection(collectionName), clazz);
    }

    /**
     * Vector store with object mapping and native function.
     *
     * @param collectionName
     *      collection name
     * @param recordClass
     *      record class
     * @return
     *      vector store
     * @param <T>
     *      type parameter
     */
    public <T> VectorStore<T> vectorStore(String collectionName, Class<T> recordClass) {
        if (!existCollection(collectionName)) throw new CollectionNotFoundException(collectionName);
        return new VectorStore<>(collection(collectionName), recordClass);
    }

    /**
     * Default vector store working with key/value.
     *
     * @param collectionName
     *      collection name
     * @return
     *      vector store.
     */
    public VectorStore<ObjectMap> vectorStore(String collectionName) {
        return vectorStore(collectionName, ObjectMap.class);
    }


}
