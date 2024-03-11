package io.stargate.sdk.v1.data;

import io.stargate.sdk.http.ServiceHttp;
import io.stargate.sdk.v1.data.domain.CollectionDefinition;
import io.stargate.sdk.v1.data.domain.ApiResponse;
import io.stargate.sdk.v1.data.domain.SimilarityMetric;
import io.stargate.sdk.v1.data.exception.DataApiCollectionNotFoundException;
import io.stargate.sdk.utils.Assert;
import io.stargate.sdk.v1.data.utils.DataApiUtils;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Stream;

import static io.stargate.sdk.utils.AnsiUtils.green;

/**
 * Client for a namespace (crud for collections).
 */
@Getter  @Slf4j
public class NamespaceClient {

    /** Collection. */
    private final String namespace;

    /** Reference to data api client. */
    private final DataApiClient dataApiClient;

    /** Resource for namespace. */
    public final Function<ServiceHttp, String> namespaceResource;

    /**
     * Full constructor.
     *
     * @param dataApiClient data api client reference
     * @param namespace namespace identifier
     */
    protected NamespaceClient(@NonNull DataApiClient dataApiClient, @NonNull String namespace) {
        this.namespace          = namespace;
        this.dataApiClient      = dataApiClient;
        this.namespaceResource  = (node) -> dataApiClient.rootResource.apply(node) + "/" + namespace;
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
    public boolean isCollectionExists(String collection) {
        return findCollections().map(CollectionDefinition::getName).anyMatch(collection::equals);
    }

    /**
     * Find Collections.
     *
     * @return
     *       a list of Collections
     */
    public Stream<CollectionDefinition> findCollections() {
        return execute("findCollections", Map.of("options", Map.of("explain", true)))
                .getStatusKeyAsList("collections", CollectionDefinition.class)
                .stream();
    }

    /**
     * Create a Collection providing a name.
     *
     * @param collection
     *      current Collection.
     * @return collection client.
     */
    public CollectionClient createCollection(String collection) {
        return this.createCollection(CollectionDefinition.builder().name(collection).build());
    }

    /**
     * Create a Collection providing a name.
     *
     * @param clazz
     *      type to be returned
     * @param <DOC>
     *       type of document in used
     * @param collection
     *      current Collection.
     * @return
     *      collection repository
     */
    public <DOC> CollectionRepository<DOC> createCollection(String collection,  Class<DOC> clazz) {
        return this.createCollection(CollectionDefinition.builder().name(collection).build(), clazz);
    }

    /**
     * Create a Collection for vector purpose
     *
     * @param collection
     *      current Collection.
     * @param dimension
     *      dimension of the vector
     * @return collection client.
     */
    public CollectionClient createCollection(String collection, int dimension) {
        return this.createCollection(CollectionDefinition.builder()
                .name(collection)
                .vector(dimension, SimilarityMetric.cosine)
                .build());
    }

    /**
     * Create a Collection providing a name.
     *
     * @param req
     *      current Collection.
     * @return collection client.
     */
    public CollectionClient createCollection(CollectionDefinition req) {
        execute("createCollection", req);
        log.info("Collection  '" + green("{}") + "' has been created", req.getName());
        return new CollectionClient(this, req.getName());
    }

    /**
     * Create a Collection providing a name.
     *
     * @param <DOC>
     *      document type
     * @param req
     *      current Collection.
     * @param clazz
     *      type of clas in used
     * @return collection client.
     */
    public <DOC> CollectionRepository<DOC> createCollection(CollectionDefinition req, Class<DOC> clazz) {
        execute("createCollection", req);
        log.info("Collection  '" + green("{}") + "' has been created", req.getName());
        return collectionRepository(req.getName(), clazz);
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
     * @return
     *      api response
     */
    private ApiResponse execute(String operation, Object payload) {
        return DataApiUtils.executeOperation(dataApiClient.getStargateHttpClient(), namespaceResource, operation, payload);
    }

    /**
     * Find a Collection from its name.
     *
     * @param collectionName
     *      collection name
     * @return
     *      collection definition if exists
     */
    public Optional<CollectionDefinition> findCollectionByName(String collectionName) {
        return findCollections()
                .filter(collection -> collection.getName().equals(collectionName))
                .findFirst();
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
    public CollectionClient collection(String collectionName) {
        if (findCollections()
                .map(CollectionDefinition::getName)
                .noneMatch(collectionName::equals)) throw new DataApiCollectionNotFoundException(collectionName);
        return new CollectionClient(this, collectionName);
    }

    /**
     * Build repository for a collection.
     *
     * @param clazz
     *      pojo class
     * @param collectionName
     *      collection name
     * @return
     *      collection repository
     * @param <T>
     *      type parameter
     */
    public <T> CollectionRepository<T> collectionRepository(String collectionName, Class<T> clazz) {
        if (!isCollectionExists(collectionName)) throw new DataApiCollectionNotFoundException(collectionName);
        return new CollectionRepository<>(collection(collectionName), clazz);
    }

}
