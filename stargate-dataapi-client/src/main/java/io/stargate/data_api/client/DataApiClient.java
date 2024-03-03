package io.stargate.data_api.client;


import io.stargate.data_api.client.exception.NamespaceNotFoundException;
import io.stargate.data_api.client.model.CreateNamespaceOptions;
import lombok.NonNull;

import java.util.stream.Stream;

/**
 * Client core for Data API (crud for namespaces).
 */
public interface DataApiClient {

    /**
     * Get a list of the database names
     *
     * @return the list of namespaces for current database.
     */
    Stream<String> listNamespaceNames();

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
     * Create a Namespace providing a name.
     *
     * @param namespace
     *      current namespace.
     * @return
     *      client for namespace
     */
    default DataApiNamespace createNamespace(@NonNull String namespace) {
        return createNamespace(namespace, CreateNamespaceOptions.simpleStrategy(1));
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
