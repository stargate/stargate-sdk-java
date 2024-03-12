package io.stargate.sdk.data.internal;

import io.stargate.sdk.data.client.DataApiClient;
import io.stargate.sdk.data.client.DataApiCollection;
import io.stargate.sdk.data.client.DataApiNamespace;
import io.stargate.sdk.data.client.model.Command;
import io.stargate.sdk.data.client.model.CommandCreateCollection;
import io.stargate.sdk.data.client.model.CommandDropCollection;
import io.stargate.sdk.data.client.model.CommandFindCollections;
import io.stargate.sdk.data.client.model.CreateCollectionOptions;
import io.stargate.sdk.data.internal.model.ApiResponse;
import io.stargate.sdk.data.internal.model.CollectionDefinition;
import io.stargate.sdk.http.LoadBalancedHttpClient;
import io.stargate.sdk.http.ServiceHttp;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;

import static io.stargate.sdk.utils.AnsiUtils.green;
import static io.stargate.sdk.utils.Assert.hasLength;
import static io.stargate.sdk.utils.Assert.notNull;

/**
 * Default implementation of the {@link DataApiNamespace}.
 */
@Slf4j @Getter
public class DataApiNamespaceImpl implements DataApiNamespace {

    /**
     * Reference to the Data APi client
     */
    private final DataApiClient apiClient;

    /**
     * Current Namespace information.
     */
    private final String namespaceName;

    /**
     * Resource for namespace.
     */
    public final Function<ServiceHttp, String> namespaceResource;

    /**
     * Constructor with api and namespace.
     *
     * @param apiClient
     *      reference to api client
     * @param namespace
     *      current namespace
     */
    public DataApiNamespaceImpl(DataApiClient apiClient, String namespace) {
        notNull(apiClient, "apiClient");
        hasLength(namespace, "namespace");
        this.apiClient     = apiClient;
        this.namespaceName = namespace;
        this.namespaceResource = (node) -> apiClient.lookup().apply(node) + "/" + namespace;
    }

    // ------------------------------------------
    // ----   General Informations           ----
    // ------------------------------------------

    /** {@inheritDoc} */
    @Override
    public String getName() {
        return namespaceName;
    }

    /** {@inheritDoc} */
    @Override
    public DataApiClient getClient() {
        return apiClient;
    }

    /** {@inheritDoc} */
    @Override
    public void drop() {
        apiClient.dropNamespace(getName());
    }

    // ------------------------------------------
    // ----     Collection CRUD              ----
    // ------------------------------------------

    /** {@inheritDoc} */
    @Override
    public Stream<String> listCollectionNames() {
        return runCommand(new CommandFindCollections())
                .getStatusKeyAsList("collections", String.class)
                .stream();
    }

    /** {@inheritDoc} */
    @Override
    public Stream<CollectionDefinition> listCollections() {
        return runCommand(new CommandFindCollections().withExplain(true))
                .getStatusKeyAsList("collections", CollectionDefinition.class)
                .stream();
    }

    /** {@inheritDoc} */
    @Override
    public <DOC> DataApiCollection<DOC> createCollection(String collectionName, CreateCollectionOptions createCollectionOptions, Class<DOC> documentClass) {
        hasLength(collectionName, "collectionName");
        notNull(documentClass, "documentClass");
        ApiResponse apiResponse = runCommand(new CommandCreateCollection()
                .withName(collectionName)
                .withOptions(createCollectionOptions));
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
        runCommand(new CommandDropCollection(collectionName));
        log.info("Collection  '" + green("{}") + "' has been deleted", collectionName);
    }

    // ------------------------------------------
    // ----           Lookup                 ----
    // ------------------------------------------

    /** {@inheritDoc} */
    @Override
    public Function<ServiceHttp, String> lookup() {
        return namespaceResource;
    }

    /** {@inheritDoc} */
    @Override
    public LoadBalancedHttpClient getHttpClient() {
        return getClient().getHttpClient();
    }

}
