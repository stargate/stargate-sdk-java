package io.stargate.sdk.data.client;

import io.stargate.sdk.data.client.exception.TooManyDocumentsException;
import io.stargate.sdk.data.client.model.misc.BulkWriteOptions;
import io.stargate.sdk.data.client.model.misc.BulkWriteResult;
import io.stargate.sdk.data.client.model.collections.CreateCollectionOptions;
import io.stargate.sdk.data.client.model.delete.DeleteResult;
import io.stargate.sdk.data.client.model.DistinctIterable;
import io.stargate.sdk.data.client.model.Document;
import io.stargate.sdk.data.client.model.Filter;
import io.stargate.sdk.data.client.model.FindIterable;
import io.stargate.sdk.data.client.model.insert.InsertManyOptions;
import io.stargate.sdk.data.client.model.insert.InsertManyResult;
import io.stargate.sdk.data.client.model.update.ReplaceOptions;
import io.stargate.sdk.data.client.model.update.UpdateOptions;
import io.stargate.sdk.data.client.model.update.UpdateResult;
import io.stargate.sdk.data.client.model.find.FindOneAndDeleteOptions;
import io.stargate.sdk.data.client.model.find.FindOneAndReplaceOptions;
import io.stargate.sdk.data.client.model.find.FindOneAndUpdateOptions;
import io.stargate.sdk.data.client.model.find.FindOneOptions;
import io.stargate.sdk.data.client.model.insert.InsertOneResult;

import java.util.List;
import java.util.Optional;

import static io.stargate.sdk.data.client.model.Filters.eq;

/**
 * Definition of operation for an Astra Collection.
 *
 * @param <DOC>
 *     Java bean mapping documents in the collections.
 */
public interface DataApiCollection<DOC> extends ApiClient {

    // ----------------------------
    // --- Global Informations ----
    // ----------------------------

    /**
     * Gets the namespace for the collection.
     *
     * @return the namespace
     */
    DataApiNamespace getNamespace();

    /**
     * Return the options of the collection (if any) with vector and indexing options.
     *
     * @return
     *      collection options
     */
    CreateCollectionOptions getOptions();

    /**
     * Get the class of documents stored in this collection.
     *
     * @return the class
     */
    Class<DOC> getDocumentClass();

    /**
     * Return the name of the collection.
     *
     * @return
     *      the name of the collection
     */
    String getName();

    /**
     * Drops the collection from the namespace/Database.
     */
    void drop();

    /**
     * Validate if a collection exists.
     *
     * @return
     *      if the collection exists
     */
    default boolean exists() {
        return getNamespace().existCollection(getName());
    }

    // --------------------------
    // ---      Find         ----
    // --------------------------

    /**
     * Find one document from a filter.
     *
     * @param filter
     *      filter
     * @return
     *      document if it exists
     */
    default Optional<Document> findOne(Filter filter) {
        return findOne(filter, new FindOneOptions());
    }

    /**
     * Create a filter by id.
     *
     * @param id
     *      value for identifier
     * @return
     *      document if it exists
     */
    default Optional<Document> findById(Object id) {
        return findOne(eq(id));
    }

    /**
     * Find one document from a filter.
     *
     * @param filter
     *      filter
     * @param options
     *      no options
     * @return
     *      document
     */
    Optional<Document> findOne(Filter filter, FindOneOptions options);

    /**
     * Finds all documents in the collection.
     *
     * @return
     *      the find iterable interface
     */
    FindIterable<DOC> find();

    /**
     * Finds all documents in the collection.
     *
     * @param resultClass
     *      the class to decode each document into
     * @param <T>
     *      the target document type of the iterable, different from default
     * @return
     *      the find iterable interface
     */
    <T> FindIterable<T> find(Class<T> resultClass);

    /**
     * Finds all documents in the collection.
     *
     * @param filter
     *      the query filter
     * @return
     *      the find iterable interface
     */
    FindIterable<DOC> find(Filter filter);

    /**
     * Finds all documents in the collection.
     *
     * @param filter
     *      the query filter
     * @param resultClass
     *      the class to decode each document into
     * @param <T>
     *      the target document type of the iterable.
     * @return
     *      the find iterable interface
     */
    <T> FindIterable<T> find(Filter filter, Class<T> resultClass);

    /**
     * Gets the distinct values of the specified field name.
     * The iteration is performed at CLIENT-SIDE and will exhaust all the collections elements.
     *
     * @param fieldName
     *      the field name
     * @param resultClass
     *      the class to cast any distinct items into.
     * @param <FIELD>
     *      the target type of the iterable.
     * @return
     *      an iterable of distinct values
     */
    <FIELD> DistinctIterable<FIELD> distinct(String fieldName, Class<FIELD> resultClass);

    /**
     * Gets the distinct values of the specified field name.
     *
     * @param fieldName
     *      the field name
     * @param filter
     *      the query filter
     * @param resultClass
     *      the class to cast any distinct items into.
     * @param <FIELD>
     *      the target type of the iterable.
     * @return
     *      an iterable of distinct values
     */
    <FIELD> DistinctIterable<FIELD> distinct(String fieldName, Filter filter, Class<FIELD> resultClass);

    // --------------------------
    // ---   Count           ----
    // --------------------------

    /**
     * Counts the number of documents in the collection.
     *
     * <p>
     * Takes in a `upperBound` option which dictates the maximum number of documents that may be present before a
     * {@link TooManyDocumentsException} is thrown. If the limit is higher than the highest limit accepted by the
     * Data API, a {@link TooManyDocumentsException} will be thrown anyway (i.e. `1000`).
     * </p>
     * <p>
     * Count operations are expensive: for this reason, the best practice is to provide a reasonable `upperBound`
     * according to the caller expectations. Moreover, indiscriminate usage of count operations for sizeable amounts
     * of documents (i.e. in the thousands and more) is discouraged in favor of alternative application-specific
     * solutions. Keep in mind that the Data API has a hard upper limit on the amount of documents it will count,
     * and that an exception will be thrown by this method if this limit is encountered.
     * </p>
     *
     * @param upperBound
     *      The maximum number of documents to count.
     * @return
     *      The number of documents in the collection.
     * @throws TooManyDocumentsException
     *      If the number of documents counted exceeds the provided limit.
     */
    long countDocuments(int upperBound) throws TooManyDocumentsException;

    /**
     * Counts the number of documents in the collection with a filter.
     *
     * <p>
     * Takes in a `upperBound` option which dictates the maximum number of documents that may be present before a
     * {@link TooManyDocumentsException} is thrown. If the limit is higher than the highest limit accepted by the
     * Data API, a {@link TooManyDocumentsException} will be thrown anyway (i.e. `1000`).
     * </p>
     * <p>
     * Count operations are expensive: for this reason, the best practice is to provide a reasonable `upperBound`
     * according to the caller expectations. Moreover, indiscriminate usage of count operations for sizeable amounts
     * of documents (i.e. in the thousands and more) is discouraged in favor of alternative application-specific
     * solutions. Keep in mind that the Data API has a hard upper limit on the amount of documents it will count,
     * and that an exception will be thrown by this method if this limit is encountered.
     * </p>
     *
     * @param filter
     *      A filter to select the documents to count. If not provided, all documents will be counted.
     * @param upperBound
     *      The maximum number of documents to count.
     * @return
     *      The number of documents in the collection.
     * @throws TooManyDocumentsException
     *      If the number of documents counted exceeds the provided limit.
     */
    long countDocuments(Filter filter, int upperBound)  throws TooManyDocumentsException;

    // --------------------------
    // ---   Insert          ----
    // --------------------------

    /**
     * Insert a single document in the collection in an atomic operation.
     *
     * <p>
     * <blockquote><b>Note:</b>If an `_id` is explicitly provided, which corresponds to a document
     * that exists already in the collection, an error is raised and the insertion fails.
     * Inserts the provided document. If the document is missing an identifier, the server will generate one.
     * </blockquote>
     * </p>
     *
     * @param document
     *     the document expressing the document to insert. The `_id` field of the document can be left out, in which case it will be created automatically.
     * @return
     *       an InsertOneResult object.
     */
    InsertOneResult insertOne(DOC document);

    /**
     * Inserts one or more documents.

     * @param documents
     *      the documents to insert
     * @return
     *      the insert many result
     * @throws IllegalArgumentException
     *      if the documents list is null or empty, or any of the documents in the list are null
     */
    InsertManyResult insertMany(List<? extends DOC> documents);

    /**
     * Inserts one or more documents.
     *
     * @param documents
     *      the documents to insert
     * @param options
     *      options to insert many documents
     * @return
     *      the insert many result
     * @throws IllegalArgumentException
     *      if the documents list is null or empty, or any of the documents in the list are null
     */
   InsertManyResult insertMany(List<? extends DOC> documents, InsertManyOptions options);

    /**
     * Executes a mix of inserts, updates, replaces, and deletes.
     *
     * @param requests
     *      the jsonCommand to execute
     * @return
     *      the result of the bulk write
     */
    BulkWriteResult bulkWrite(List<String> requests);

    /**
     * Executes a mix of inserts, updates, replaces, and deletes.
     *
     * @param options
     *      if requests must be ordered or not
     * @param requests
     *      the jsonCommand to execute
     * @return
     *      the result of the bulk write
     */
    BulkWriteResult bulkWrite(List<String> requests, BulkWriteOptions options);

    // --------------------------
    // ---   Delete          ----
    // --------------------------

    /**
     * Removes at most one document from the collection that matches the given filter.
     * If no documents match, the collection is not modified.
     *
     * @param filter
     *      the query filter to apply the delete operation
     * @return
     *      the result of the remove one operation
     *
     */
    DeleteResult deleteOne(Filter filter);

    /**
     * Removes all documents from the collection that match the given query filter. If no documents match, the collection is not modified.
     *
     * @param filter
     *      the query filter to apply the delete operation
     * @return
     *      the result of the remove many operation
     */
    DeleteResult deleteMany(Filter filter);

    /**
     * Removes all documents from the collection that match the given query filter. If no documents match, the collection is not modified.
     *
     * @return
     *      the result of the remove many operation
     */
    DeleteResult deleteAll();

    /**
     * Atomically find a document and remove it.
     *
     * @param filter
     *      the query filter to find the document with
     * @return
     *      the document that was removed.  If no documents matched the query filter, then null will be returned
     */
    Optional<DOC> findOneAndDelete(Filter filter);

    /**
     * Atomically find a document and remove it.
     *
     * @param filter
     *      the query filter to find the document with
     * @param options
     *      the options to apply to the operation
     * @return
     *      the document that was removed.  If no documents matched the query filter, then null will be returned
     */
    Optional<DOC> findOneAndDelete(Filter filter, FindOneAndDeleteOptions options);

    // --------------------------
    // ---   Update/Replace  ----
    // --------------------------

    /**
     * Replace a document in the collection according to the specified arguments.
     * <p>Use this method to replace a document using the specified replacement argument.</p>
     *
     * @param filter
     *      the query filter to apply the replace operation
     * @param replacement
     *      the replacement document
     * @return
     *      result of the replace one operation
     */
    UpdateResult replaceOne(Filter filter, DOC replacement);

    /**
     * Replace a document in the collection according to the specified arguments.
     *
     * @param filter
     *      the query filter to apply the replace operation
     * @param replacement
     *      the replacement document
     * @param replaceOptions
     *      the options to apply to the replace operation
     * @return
     *      the result of the replace one operation
     */
    UpdateResult replaceOne(Filter filter, DOC replacement, ReplaceOptions replaceOptions);

    /**
     * Update a single document in the collection according to the specified arguments.
     *
     * @param filter
     *      a document describing the query filter, which may not be null.
     * @param update
     *      a document describing the update, which may not be null. The update to apply must include at least one update operator.
     * @return
     *      the result of the update one operation
     */
    UpdateResult updateOne(Filter filter, Object update);

    /**
     * Update a single document in the collection according to the specified arguments.
     *
     * @param filter
     *      a document describing the query filter, which may not be null.
     * @param update
     *      a document describing the update, which may not be null. The update to apply must include at least one update operator.
     * @param updateOptions
     *      the options to apply to the update operation
     * @return
     *      the result of the update one operation
     */
    UpdateResult updateOne(Filter filter, Object update, UpdateOptions updateOptions);

    /**
     * Update all documents in the collection according to the specified arguments.
     *
     * @param filter
     *      a document describing the query filter, which may not be null.
     * @param update
     *      a document describing the update, which may not be null. The update to apply must include only update operators.
     * @return
     *      the result of the update many operation
     */
    UpdateResult updateMany(Filter filter, Object update);

    /**
     * Update all documents in the collection according to the specified arguments.
     *
     * @param filter
     *      a document describing the query filter, which may not be null.
     * @param update
     *      a document describing the update, which may not be null. The update to apply must include only update operators.
     * @param updateOptions
     *      the options to apply to the update operation
     * @return
     *      the result of the update many operation
     */
    UpdateResult updateMany(Filter filter, Object update, UpdateOptions updateOptions);

    /**
     * Atomically find a document and replace it.
     *
     * @param filter
     *      the query filter to apply the replace operation
     * @param replacement
     *      the replacement document
     * @return
     *      the document that was replaced.  Depending on the value of the {@code returnOriginal} property, this will either be the document as it was before the update or as it is after the update.  If no documents matched the query filter, then null will be returned
     */
    Optional<DOC> findOneAndReplace(Filter filter, DOC replacement);

    /**
     * Atomically find a document and replace it.
     *
     * <p>Note: Supports retryable writes on MongoDB server versions 3.6 or higher when the retryWrites setting is enabled.</p>
     * @param filter
     *      the query filter to apply the replace operation
     * @param replacement
     *      the replacement document
     * @param options
     *      the options to apply to the operation
     * @return
     *      the document that was replaced.  Depending on the value of the {@code returnOriginal} property, this will either be the
     * document as it was before the update or as it is after the update.  If no documents matched the query filter, then null will be
     * returned
     */
    Optional<DOC>  findOneAndReplace(Filter filter, DOC replacement, FindOneAndReplaceOptions options);

    /**
     * Atomically find a document and update it.
     *
     * <p>Note: Supports retryable writes on MongoDB server versions 3.6 or higher when the retryWrites setting is enabled.</p>
     * @param filter
     *      a document describing the query filter, which may not be null.
     * @param update
     *      a document describing the update, which may not be null. The update to apply must include at least one update operator.
     * @return the document that was updated before the update was applied.  If no documents matched the query filter, then null will be
     * returned
     */
    Optional<DOC> findOneAndUpdate(Filter filter, Object update);

    /**
     * Atomically find a document and update it.
     *
     * @param filter
     *      a document describing the query filter, which may not be null.
     * @param update
     *      a document describing the update, which may not be null. The update to apply must include at least one update
     *               operator.
     * @param options
     *      the options to apply to the operation
     * @return
     *      the document that was updated.  Depending on the value of the {@code returnOriginal} property, this will either be the
     * document as it was before the update or as it is after the update.  If no documents matched the query filter, then null will be
     * returned
     */
    Optional<DOC> findOneAndUpdate(Filter filter, Object update, FindOneAndUpdateOptions options);

}
