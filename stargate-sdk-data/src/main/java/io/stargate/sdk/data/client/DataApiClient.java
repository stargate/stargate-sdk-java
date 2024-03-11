package io.stargate.sdk.data.client;


import io.stargate.sdk.data.client.exception.NamespaceNotFoundException;
import io.stargate.sdk.data.client.model.CreateNamespaceOptions;
import io.stargate.sdk.data.internal.model.NamespaceInformation;

import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

/**
 * Client core for Data API (crud for namespaces).
 */
public interface DataApiClient extends ApiClient {

    /**
     * Get a list of the namespace names
     *
     * @return the list of namespaces for current database.
     */
    Stream<String> listNamespaceNames();

    /**
     * Get a list of the namespace names in asynchronous way
     *
     * @return the list of namespaces for current database.
     */
    default CompletableFuture<Stream<String>> listNamespaceNamesAsync() {
        return CompletableFuture.supplyAsync(this::listNamespaceNames);
    }

    /**
     * Return a list of the keyspace with their replication information.
     *
     * @return
     *      list of namespace information
     */
    Stream<NamespaceInformation> listNamespaces();

    /**
     * Return a list of the keyspace with their replication information asynchronously.
     *
     * @return
     *      list of namespace information
     */
    default CompletableFuture<Stream<NamespaceInformation>> listNamespacesAsync() {
        return CompletableFuture.supplyAsync(this::listNamespaces);
    }

    /**
     * Gets a {@link DataApiNamespace} instance for the given namespace name.
     *
     * @param namespaceName
     *      the name of the namespace to retrieve
     * @return
     *      a {@code DataApiNamespace} representing the specified database
     * @throws NamespaceNotFoundException
     *      error is namespace is not found.
     */
    DataApiNamespace getNamespace(String namespaceName);

    /**
     * Drop a namespace, no error if it does not exist.
     *
     * @param namespace
     *      current namespace
     */
    void dropNamespace(String namespace);

    /**
     * Return a list of the keyspace with their replication information asynchronously.
     */
    default void dropNamespaceAsync(String namespace) {
        CompletableFuture.runAsync(() -> dropNamespace(namespace));
    }

    /**
     * Create a Namespace providing all options.
     *
     * @param namespace
     *      current namespace
     * @param options
     *      all namespace options
     * @return
     *      client for namespace
     */
    DataApiNamespace createNamespace(String namespace, CreateNamespaceOptions options);

    /**
     * Create a Namespace asynchronously
     *
     * @param namespace
     *      current namespace
     * @param options
     *      all namespace options
     * @return
     *      client for namespace
     */
    default CompletableFuture<DataApiNamespace> createNamespaceAsync(String namespace, CreateNamespaceOptions options) {
        return CompletableFuture.supplyAsync(() -> createNamespace(namespace, options));
    }

    /**
     * Create a Namespace providing a name.
     *
     * @param namespace
     *      current namespace.
     * @return
     *      client for namespace
     */
    default DataApiNamespace createNamespace(String namespace) {
        return createNamespace(namespace, CreateNamespaceOptions.simpleStrategy(1));
    }

    /**
     * Create a Namespace providing a name.
     *
     * @param namespace
     *      current namespace.
     * @return
     *      client for namespace
     */
    default CompletableFuture<DataApiNamespace> createNamespaceAsync(String namespace) {
        return CompletableFuture.supplyAsync(() -> createNamespace(namespace, CreateNamespaceOptions.simpleStrategy(1)));
    }

    /**
     * Evaluate if a namespace exists.
     *
     * @param namespace
     *      namespace name.
     * @return
     *      if namespace exists
     */
    default boolean isNamespaceExists(String namespace) {
        return listNamespaceNames().anyMatch(namespace::equals);
    }



}
