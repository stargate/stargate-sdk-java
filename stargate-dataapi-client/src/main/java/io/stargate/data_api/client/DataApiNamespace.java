package io.stargate.data_api.client;

import io.stargate.data_api.client.model.CreateCollectionOptions;
import io.stargate.data_api.client.model.SimilarityMetric;
import io.stargate.data_api.internal.model.CreateCollectionRequest;
import io.stargate.data_api.client.model.Document;
import lombok.NonNull;

import java.util.stream.Stream;

/**
 * Class to interact with a Namespace.
 */
public interface DataApiNamespace {

    /**
     * Gets the name of the database.
     *
     * @return the database name
     */
    String getName();

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
    DataApiCollection<Document> getCollection(@NonNull String collectionName);

    /**
     * Gets a collection, with a specific default document class.
     *
     * @param collectionName
     *      the name of the collection to return
     * @param documentClass
     *      the default class to cast any documents returned from the database into.
     * @param <TDocument>
     *      the type of the class to use instead of {@code Document}.
     * @return
     *      the collection
     */
    <TDocument> DataApiCollection<TDocument> getCollection(@NonNull String collectionName, @NonNull Class<TDocument> documentClass);

    /**
     * Executes the given command in the context of the current database.
     *
     * @param command
     *      the command to be run
     * @return
     *      the command result
     */
    Document runCommand(@NonNull Object command);

    /**
     * Executes the given command in the context of the current database.
     *
     * @param command
     *      the command to be run
     * @param resultClass
     *      the class to decode each document into
     * @param <TResult>
     *      the type of the class to use instead of {@code Document}.
     * @return
     *      the command result
     */
    <TResult> TResult runCommand(@NonNull  Object command, @NonNull  Class<TResult> resultClass);

    /**
     * Drops this namespace
     */
    void drop();

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
    Stream<CreateCollectionRequest> listCollections();

    /**
     * Delete a collection.
     *
     * @param collectionName
     *      collection name
     */
    void dropCollection(@NonNull String collectionName);

    /**
     * Create a new collection with the given name.
     *
     * @param collectionName
     *      the name for the new collection to create
     */
    default DataApiCollection<Document> createCollection(@NonNull String collectionName) {
        return createCollection(collectionName, null);
    }

    /**
     * Create a new collection with the selected options
     *
     * @param collectionName
     *      the name for the new collection to create
     * @param createCollectionOptions
     *      various options for creating the collection
     */
    DataApiCollection<Document> createCollection(@NonNull String collectionName, CreateCollectionOptions createCollectionOptions);

    /**
     * Create a new collection with the selected options
     *
     * @param collectionName
     *      the name for the new collection to create
     * @param dimension
     *      the dimension of the vector
     * @param metric
     *      the similarity metric
     */
    default DataApiCollection<Document> createCollectionVector(@NonNull String collectionName, int dimension, @NonNull SimilarityMetric metric) {
        return createCollection(collectionName, CreateCollectionOptions.builder()
                .withVectorDimension(dimension)
                .withVectorSimilarityMetric(metric)
                .build());
    }

}
