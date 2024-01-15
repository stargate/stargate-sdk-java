package io.stargate.sdk.data;

import io.stargate.sdk.core.domain.Page;
import io.stargate.sdk.data.domain.odm.Document;
import io.stargate.sdk.data.domain.odm.DocumentResult;
import io.stargate.sdk.data.domain.query.DeleteQuery;
import io.stargate.sdk.data.domain.query.Filter;
import io.stargate.sdk.data.domain.query.SelectQuery;
import lombok.Getter;
import lombok.NonNull;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Crud repository with Json Api
 *
 * @param <DOC>
 *     current bean
 */
@Getter
public class CollectionRepository<DOC> {

    /**
     * Raw collection client.
     */
    protected final CollectionClient collectionClient;

    /** Keep ref to the generic. */
    protected final Class<DOC> docClass;

    /**
     * Default constructor.
     *
     * @param col
     *      collection client parent
     * @param clazz
     *      working bean class
     */
    public CollectionRepository(CollectionClient col, Class<DOC> clazz) {
        this.collectionClient = col;
        this.docClass  = clazz;
    }

    /**
     * Return name of the store.
     *
     * @return
     *      store name
     */
    public String getName() {
        return collectionClient.getCollection();
    }

    /**
     * Check existence of a document from its id.
     * Projection to make it as light as possible.
     *
     * @param id
     *      document identifier
     * @return
     *      existence status
     */
    public boolean exists(String id) {
        return collectionClient.isDocumentExists(id);
    }

    // --------------------------
    // ---      Insert       ----
    // --------------------------

    /**
     * Save a NEW RECORD with a defined id.
     * @param bean
     *      current object
     * @return
     *      generated identifier
     */
    public DocumentMutationResult<DOC> insert(Document<DOC> bean) {
        return collectionClient.insertOne(bean);
    }

    /**
     * Save a NEW RECORD with a defined id.
     * @param bean
     *      current object
     * @return
     *      generated identifier
     */
    public CompletableFuture<DocumentMutationResult<DOC>> insertASync(Document<DOC> bean) {
        return collectionClient.insertOneASync(bean);
    }

    // --------------------------
    // ---    Insert All     ----
    // --------------------------

    // --------------------------
    // ---    Insert All     ----
    // --------------------------

    /**
     * Low level insertion of multiple records, they should not exist, or it will fail.
     *
     * @param documents
     *      list of documents
     * @return
     *      list of ids
     */
    public final List<DocumentMutationResult<DOC>> insertAll(List<Document<DOC>> documents) {
        return insertAllDistributed(documents, 20, 1);
    }

    /**
     * Low level insertion of multiple records, they should not exist, or it will fail.
     *
     * @param documents
     *      list of documents
     * @param chunkSize
     *      how many document per chunk
     * @param concurrency
     *      how many thread in parallel
     * @return
     *      list of ids
     */
    public final List<DocumentMutationResult<DOC>> insertAllDistributed(List<Document<DOC>> documents, int chunkSize, int concurrency) {
        return collectionClient.insertManyChunked(documents, chunkSize, concurrency);
    }


    // --------------------------
    // ---  Save / Upsert    ----
    // --------------------------

    /**
     * Upsert a record
     *
     * @param current
     *      object Mapping
     * @return
     *      an unique identifier for the document
     */
    public final DocumentMutationResult<DOC> save(Document<DOC> current) {
        return collectionClient.upsertOne(current);
    }

    /**
     * Upsert a record
     *
     * @param current
     *      object Mapping
     * @return
     *      an unique identifier for the document
     */
    public final CompletableFuture<DocumentMutationResult<DOC>> saveASync(Document<DOC> current) {
        return collectionClient.upsertOneASync(current);
    }

    // --------------------------
    // ---    saveAll        ----
    // --------------------------

    /**
     * Create a new document a generating identifier.
     *
     * @param documentList
     *      object Mapping
     * @return
     *      an unique identifier for the document
     */
    public final List<DocumentMutationResult<DOC>> saveAll(List<Document<DOC>> documentList) {
        return collectionClient.upsertMany(documentList);
    }

    /**
     * Create a new document a generating identifier asynchronously
     *
     * @param documentList
     *      object Mapping
     * @return
     *      an unique identifier for the document
     */
    public final CompletableFuture<List<DocumentMutationResult<DOC>>> saveAllASync(List<Document<DOC>> documentList) {
        return collectionClient.upsertManyASync(documentList);
    }

    /**
     * Create a new document a generating identifier.
     *
     * @param documentList
     *      object Mapping
     * @param chunkSize
     *      size of the chunk to process items
     * @param concurrency
     *      concurrency to process items
     * @return
     *      an unique identifier for the document
     */
    public final List<DocumentMutationResult<DOC>> saveAllDistributed(List<Document<DOC>> documentList, int chunkSize, int concurrency) {
        return collectionClient.upsertManyChunked(documentList, chunkSize, concurrency);
    }

    /**
     * Create a new document a generating identifier asynchronously
     *
     * @param documentList
     *      object Mapping
     * @param chunkSize
     *      size of the chunk to process items
     * @param concurrency
     *      concurrency to process items
     * @return
     *      an unique identifier for the document
     */
    public final CompletableFuture<List<DocumentMutationResult<DOC>>> saveAllDistributedASync(List<Document<DOC>> documentList, int chunkSize, int concurrency) {
        return collectionClient.upsertManyChunkedASync(documentList, chunkSize, concurrency);
    }

    // --------------------------
    // ---      Count        ----
    // --------------------------

    /**
     * Count Document request.
     *
     * @return
     *      number of document.
     */
    public final int count() {
        return count(null);
    }

    /**
     * Count Document request.
     *
     * @param jsonFilter
     *      a filter for the count
     * @return
     *      number of document.
     */
    public final int count(Filter jsonFilter) {
        return collectionClient.countDocuments(jsonFilter);
    }

    // --------------------------
    // ---      Find         ----
    // --------------------------

    /**
     * Find by id.
     *
     * @param query
     *      query to retrieve documents and vector
     * @return
     *      object if presents
     */
    public Optional<DocumentResult<DOC>> find(@NonNull SelectQuery query) {
        return collectionClient.findOne(query, docClass);
    }

    /**
     * Find by id.
     *
     * @param id
     *      identifier
     * @return
     *      object if presents
     */
    public Optional<DocumentResult<DOC>> findById(@NonNull String id) {
        return collectionClient.findById(id, docClass);
    }

    /**
     * Find all item in the collection.
     *
     * @return
     *      retrieve all items
     */
    public Stream<DocumentResult<DOC>> search() {
        return collectionClient.findAll(docClass);
    }

    /**
     * Find all item in the collection.
     *
     * @param query
     *      search with a query
     * @return
     *      retrieve all items
     */
    public Stream<DocumentResult<DOC>> search(SelectQuery query) {
        return collectionClient.find(query, docClass);
    }

    /**
     * Find a page in the collection.
     *
     * @param query
     *      current query
     * @return
     *      page of records
     */
    public Page<DocumentResult<DOC>> searchPage(SelectQuery query) {
        return collectionClient.findPage(query, docClass);
    }

    // --------------------------
    // ---     Delete        ----
    // --------------------------

    /**
     * Delete a document from id or vector
     * .
     * @param document
     *      document
     * @return
     *      if document has been deleted.
     */
    public boolean delete(@NonNull Document<DOC> document) {
        if (document.getId() != null)     return collectionClient.deleteById(document.getId()) > 0;
        if (document.getVector() != null) return collectionClient.deleteByVector(document.getVector()) > 0;
        throw new IllegalArgumentException("Cannot delete record without id or vector");
    }

    /**
     * Delete all documents
     *
     * @return
     *     number of document deleted
     */
    public int deleteAll() {
        return collectionClient.deleteAll();
    }

    /**
     * Use parallelism and async to delete all records.
     *
     * @param documents
     *      list of records
     * @return
     *      number of records deleted
     */
    public int deleteAll(List<Document<DOC>> documents) {
        List<CompletableFuture<Integer>> futures = documents.stream()
                .map(record -> CompletableFuture.supplyAsync(() -> delete(record) ? 1 : 0))
                .collect(Collectors.toList());
        return futures.stream()
                .map(CompletableFuture::join) // This will wait for the result of each future
                .mapToInt(Integer::intValue)
                .sum();
    }

    /**
     * Delete item through a query.
     *
     * @param deleteQuery
     *      delete query
     * @return
     *       number of records deleted
     */
    public int deleteAll(DeleteQuery deleteQuery) {
        return collectionClient.deleteMany(deleteQuery);
    }

    // ------------------------------
    // --- OPERATIONS VECTOR     ----
    // ------------------------------

    /**
     * Find by vector
     *
     * @param vector
     *      vector
     * @return
     *      object if presents
     */
    public Optional<DocumentResult<DOC>> findByVector(float[] vector) {
        return collectionClient.findOneByVector(vector, docClass);
    }

    /**
     * Delete by vector
     *
     * @param vector
     *      vector
     * @return
     *      if object deleted
     */
    public boolean deleteByVector(float[] vector) {
        return collectionClient.deleteByVector(vector) > 0;
    }


    /**
     * Delete by vector
     *
     * @param id
     *      id
     * @return
     *      if object deleted
     */
    public boolean deleteById(String id) {
        return collectionClient.deleteById(id) > 0;
    }

    // ------------------------------
    // ---  Similarity Search    ----
    // ------------------------------

    /**
     * Search similarity from the vector and a limit, if a limit / no paging
     *
     * @param vector
     *      vector
     * @param metadataFilter
     *      metadata filtering
     * @return
     *      page of results
     */
    public Page<DocumentResult<DOC>> findVector(float[] vector, Filter metadataFilter) {
        return collectionClient.findVectorPage(vector, metadataFilter, null, null, docClass);
    }

    /**
     * Search similarity from the vector and a limit, if a limit / no paging
     *
     * @param vector
     *      vector
     * @param limit
     *      return count
     * @return
     *      page of results
     */
    public List<DocumentResult<DOC>> findVector(float[] vector, Integer limit) {
        return collectionClient.findVectorPage(vector, null, limit, null, docClass).getResults();
    }

    /**
     * Search similarity from the vector and a limit, if a limit / no paging
     *
     * @param vector
     *      vector
     * @param limit
     *      return count
     * @param metadataFilter
     *      metadata filtering
     * @return
     *      page of results
     */
    public List<DocumentResult<DOC>> findVector(float[] vector, Filter metadataFilter, Integer limit) {
        return collectionClient.findVectorPage(vector, metadataFilter, limit, null, docClass).getResults();
    }
}
