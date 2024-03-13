package io.stargate.sdk.data.client;

import io.stargate.sdk.data.client.model.collections.CreateCollectionOptions;
import io.stargate.sdk.data.internal.model.CollectionDefinition;
import io.stargate.sdk.data.client.model.Document;

import java.util.stream.Stream;

/**
 * Class to interact with a Namespace.
 */
public interface DataApiNamespace extends ApiClient {

    // ------------------------------------------
    // ----   General Informations           ----
    // ------------------------------------------

    /**
     * Gets the name of the database.
     *
     * @return the database name
     */
    String getName();

    /**
     * Gets the name of the database.
     *
     * @return the database name
     */
    DataApiClient getClient();

    // ------------------------------------------
    // ----     Collection CRUD              ----
    // ------------------------------------------

    /**
     * Gets the names of all the collections in this database.
     *
     * @return
     *      a stream containing all the names of all the collections in this database
     */
    Stream<String> listCollectionNames();

    /**
     * Finds all the collections in this database.
     *
     * @return
     *  list of collection definitions
     */
    Stream<CollectionDefinition> listCollections();

    /**
     * Evaluate if a collection exists.
     *
     * @param collection
     *      namespace name.
     * @return
     *      if namespace exists
     */
    default boolean existCollection(String collection) {
        return listCollectionNames().anyMatch(collection::equals);
    }

    /**
     * Gets a collection.
     *
     * @param collectionName
     *      the name of the collection to return
     * @return
     *      the collection
     * @throws IllegalArgumentException
     *      if collectionName is invalid
     */
    default DataApiCollection<Document> getCollection(String collectionName) {
        return getCollection(collectionName, Document.class);
    }

    /**
     * Gets a collection, with a specific default document class.
     *
     * @param collectionName
     *      the name of the collection to return
     * @param documentClass
     *      the default class to cast any documents returned from the database into.
     * @param <DOC>
     *      the type of the class to use instead of {@code Document}.
     * @return
     *      the collection
     */
    <DOC> DataApiCollection<DOC> getCollection(String collectionName, Class<DOC> documentClass);

    /**
     * Drops this namespace
     */
    void drop();

    /**
     * Create a new collection with the given name.
     *
     * @param collectionName
     *      the name for the new collection to create
     */
    default DataApiCollection<Document> createCollection(String collectionName) {
        return createCollection(collectionName, null, Document.class);
    }

    /**
     * Create a new collection with the given name.
     *
     * @param collectionName
     *      the name for the new collection to create
     */
    default <DOC> DataApiCollection<DOC> createCollection(String collectionName, Class<DOC> dpcumentClass) {
        return createCollection(collectionName, null, dpcumentClass);
    }

    /**
     * Create a new collection with the given name.
     *
     * @param collectionName
     *      the name for the new collection to create
     * @param createCollectionOptions
     *      various options for creating the collection
     */
    default DataApiCollection<Document> createCollection(String collectionName, CreateCollectionOptions createCollectionOptions) {
        return createCollection(collectionName, createCollectionOptions, Document.class);
    }

    /**
     * Create a new collection with the selected options
     *
     * @param collectionName
     *      the name for the new collection to create
     * @param createCollectionOptions
     *      various options for creating the collection
     */
    <DOC> DataApiCollection<DOC> createCollection(String collectionName, CreateCollectionOptions createCollectionOptions, Class<DOC> documentClass);

    /**
     * Delete a collection.
     *
     * @param collectionName
     *      collection name
     */
    void dropCollection(String collectionName);

}
