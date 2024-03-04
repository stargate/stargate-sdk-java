package io.stargate.data_api.internal;

import io.stargate.data_api.client.DataApiCollection;
import io.stargate.data_api.client.DataApiNamespace;
import io.stargate.data_api.client.model.CreateCollectionOptions;
import io.stargate.data_api.internal.model.ApiResponse;
import io.stargate.data_api.internal.model.CreateCollectionRequest;
import io.stargate.sdk.http.ServiceHttp;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;

import static io.stargate.data_api.internal.DataApiUtils.executeOperation;
import static io.stargate.sdk.utils.AnsiUtils.green;
import static io.stargate.sdk.utils.Assert.hasLength;
import static io.stargate.sdk.utils.Assert.notNull;

@Slf4j
@Getter
public class DataApiNamespaceImpl implements DataApiNamespace {

    private final DataApiClientImpl apiClient;

    private final String namespaceName;

    /** Resource for namespace. */
    public final Function<ServiceHttp, String> namespaceResource;

    public DataApiNamespaceImpl(DataApiClientImpl apiClient, String namespace) {
        notNull(apiClient, "apiClient");
        hasLength(namespace, "namespace");
        this.apiClient = apiClient;
        this.namespaceName = namespace;
        this.namespaceResource = (node) -> apiClient.rootResource.apply(node) + "/" + namespace;
    }

    /** {@inheritDoc} */
    @Override
    public String getName() {
        return namespaceName;
    }

    /** {@inheritDoc} */
    @Override
    public void drop() {
        apiClient.dropNamespace(getName());
    }

    /** {@inheritDoc} */
    @Override
    public Stream<String> listCollectionNames() {
        return execute("findCollections", "{}")
                .getStatusKeyAsList("collections", String.class)
                .stream();
    }

    /** {@inheritDoc} */
    @Override
    public Stream<CreateCollectionRequest> listCollections() {
        return execute("findCollections", Map.of("options", Map.of("explain", true)))
                .getStatusKeyAsList("collections", CreateCollectionRequest.class)
                .stream();
    }

    @Override
    public <DOC> DataApiCollection<DOC> createCollection(String collectionName, CreateCollectionOptions createCollectionOptions, Class<DOC> documentClass) {
        hasLength(collectionName, "collectionName");
        notNull(documentClass, "documentClass");
        CreateCollectionRequest request = new CreateCollectionRequest();
        request.setName(collectionName);
        request.setOptions(createCollectionOptions);
        ApiResponse apiResponse = execute("createCollection", request);
        if (apiResponse.getErrors() != null && !apiResponse.getErrors().isEmpty()) {
            apiResponse.getErrors().get(0).throwDataApiException();
        }
        log.info("Collection  '" + green("{}") + "' has been created", collectionName);
        return getCollection(collectionName, documentClass);
    }

    /** {@inheritDoc} */
    @Override
    public <DOC> DataApiCollection<DOC> getCollection(String collectionName, @NonNull Class<DOC> documentClass) {
        hasLength(collectionName, "collectionName");
        notNull(documentClass, "documentClass");
        return new DataApiCollectionImpl<>(this, collectionName, documentClass);
    }

    /** {@inheritDoc} */
    @Override
    public void dropCollection(String collectionName) {
        hasLength(collectionName, "collectionName");
        execute("deleteCollection", Map.of("name", collectionName));
        log.info("Collection  '" + green("{}") + "' has been deleted", collectionName);
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
        return executeOperation(apiClient.getStargateHttpClient(), namespaceResource, operation, payload);
    }
}
