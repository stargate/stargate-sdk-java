package io.stargate.sdk.json;

import io.stargate.sdk.core.domain.Page;
import io.stargate.sdk.json.domain.DeleteQuery;
import io.stargate.sdk.json.domain.Filter;
import io.stargate.sdk.json.domain.SelectQuery;
import io.stargate.sdk.json.domain.UpdateQuery;
import io.stargate.sdk.json.domain.odm.Document;
import io.stargate.sdk.json.domain.odm.Result;
import lombok.NonNull;

import java.util.ArrayList;
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
     *      sotre name
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
    public String insert(Document<DOC> bean) {
        return collectionClient.insertOne(bean);
    }

    // --------------------------
    // ---      SaveOne      ----
    // --------------------------

    /**
     * Save by record.
     *
     * @param current
     *      object Mapping
     * @return
     *      an unique identifier for the document
     */
    public final String save(@NonNull Document<DOC> current) {
        String id = current.getId();
        if (id == null || !exists(id)) {
            return collectionClient.insertOne(current);
        }
        // Already Exist
        collectionClient.findOneAndReplace(UpdateQuery.builder()
              .where("_id")
              .isEqualsTo(id)
              .replaceBy(current.toJsonDocument())
              .build());
        return id;
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
    public final List<String> saveAll(@NonNull List<Document<DOC>> documentList) {
        if (documentList.isEmpty()) return new ArrayList<>();
        return documentList.stream().map(this::save).collect(Collectors.toList());
    }

    /**
     * Low level insertion of multiple records
     *
     * @param documents
     *      list of documents
     * @return
     *      list of ids
     */
    public final List<String> insertAll(List<Document<DOC>> documents) {
        if (documents == null || documents.isEmpty()) return new ArrayList<>();
        return collectionClient.insertMany(documents.stream()
                .map(Document::toJsonDocument)
                .collect(Collectors.toList()));
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
     * Find by Id.
     *
     * @param query
     *      query to retrieve documents and vector
     * @return
     *      object if presents
     */
    public Optional<Result<DOC>> find(@NonNull SelectQuery query) {
        return collectionClient.findOne(query, docClass);
    }

    /**
     * Find by Id.
     *
     * @param id
     *      identifier
     * @return
     *      object if presents
     */
    public Optional<Result<DOC>> findById(@NonNull String id) {
        return collectionClient.findById(id, docClass);
    }

    /**
     * Find all item in the collection.
     *
     * @return
     *      retrieve all items
     */
    public Stream<Result<DOC>> search() {
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
    public Stream<Result<DOC>> search(SelectQuery query) {
        return collectionClient.query(query, docClass);
    }

    /**
     * Find a page in the collection.
     *
     * @param query
     *      current query
     * @return
     *      page of records
     */
    public Page<Result<DOC>> searchPage(SelectQuery query) {
        return collectionClient.queryForPage(query, docClass);
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
     *      delete queru
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
    public Optional<Result<DOC>> findByVector(@NonNull float[] vector) {
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
    public Page<Result<DOC>> similaritySearchPage(float[] vector, Filter metadataFilter) {
        return collectionClient.similaritySearch(vector, metadataFilter, null, null, docClass);
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
    public List<Result<DOC>> similaritySearch(float[] vector, Integer limit) {
        return collectionClient.similaritySearch(vector, null, limit, null, docClass).getResults();
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
    public List<Result<DOC>> similaritySearch(float[] vector, Filter metadataFilter, Integer limit) {
        return collectionClient.similaritySearch(vector, metadataFilter, limit, null, docClass).getResults();
    }

    /**
     * Gets collectionClient.
     *
     * @return value of collectionClient
     */
    public CollectionClient getCollectionClient() {
        return collectionClient;
    }
}
