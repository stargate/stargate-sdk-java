package io.stargate.sdk.json;

import io.stargate.sdk.core.domain.Page;
import io.stargate.sdk.json.domain.DeleteQuery;
import io.stargate.sdk.json.domain.Filter;
import io.stargate.sdk.json.domain.JsonDocument;
import io.stargate.sdk.json.domain.JsonResultUpdate;
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
 * @param <BEAN>
 *     current bean
 */
public class CollectionRepository<BEAN> {

    /**
     * Raw collection client.
     */
    protected final JsonCollectionClient collectionClient;

    /** Keep ref to the generic. */
    protected final Class<BEAN> docClass;

    /**
     * Default constructor.
     *
     * @param col
     *      collection client parent
     * @param clazz
     *      working bean class
     */
    public CollectionRepository(JsonCollectionClient col, Class<BEAN> clazz) {
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

    // --------------------------
    // ---     Exists        ----
    // --------------------------

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
        return  collectionClient.findOne(SelectQuery.builder()
                .select("_id")
                .where("_id")
                .isEqualsTo(id).build()).isPresent();
    }

    // --------------------------
    // ---    insertOne      ----
    // --------------------------

    /**
     * Insert a full JSON thing.
     *
     * @param jsonString
     *      json String
     * @return
     *      value
     */
    public final String insert(@NonNull String jsonString) {
        return collectionClient.insertOne(jsonString);
    }

    /**
     * Insert with a Json Document.
     *
     * @param jsonDocument
     *      current bean
     * @return
     *      new id
     */
    public final String insert(@NonNull JsonDocument jsonDocument) {
        return collectionClient.insertOne(jsonDocument);
    }

    /**
     * Create a new document a generating identifier.
     *
     * @param current
     *      delete a document
     * @return
     *      generated id
     */
    public final String insert(@NonNull BEAN current) {
        return collectionClient.insertOne(current, null);
    }

    /**
     * Create a new document with the id, if I already exist an error will occur.
     *
     * @param id
     *      document identifier
     * @param current
     *      document to create
     * @return
     *      generated id
     */
    public final String insert( @NonNull String id, @NonNull BEAN current) {
        return collectionClient.insertOne(id, current, null);
    }

    /**
     * Insert with a Json Document.
     *
     * @param bean
     *      current bean
     * @return
     *      new id
     */
    public final String insert(@NonNull Document<BEAN> bean) {
        return collectionClient.insertOne(bean.toJsonDocument());
    }

    // --------------------------
    // ---      SaveOne      ----
    // --------------------------

    /**
     * Generate a new document with a new id.
     *
     * @param current
     *      current object to create
     * @return
     *      generated id
     */
    public final String save(@NonNull BEAN current) {
        return insert(current);
    }

    /**
     * Create a new document with the id, if I already exist an error will occur.
     *
     * @param id
     *      identifier
     * @param current
     *      current bean
     * @return
     *      if the document has been updated
     */
    public boolean save(String id, @NonNull BEAN current) {
        if (!exists(id)) {
            insert(id, current);
            return true;
        }
        JsonResultUpdate res = collectionClient
                .findOneAndReplace(UpdateQuery.builder()
                .where("_id")
                .isEqualsTo(id)
                .replaceBy(new JsonDocument(id, current))
                .build());
        return res.getUpdateStatus().getModifiedCount() > 0;
    }

    /**
     * Save by record.
     *
     * @param current
     *      object Mapping
     * @return
     *      an unique identifier for the document
     */
    public final String save(@NonNull Document<BEAN> current) {
        String id = current.getId();
        if (id == null || !exists(id)) {
            return collectionClient.insertOne(current.getId(), current.getData(), current.getVector());
        }
        // Already Exist
        collectionClient.findOneAndReplace(UpdateQuery.builder()
              .where("_id")
              .isEqualsTo(id)
              .replaceBy(current.toJsonDocument())
              .build());
        return id;
    }

    /**
     * Save a json document.
     *
     * @param doc
     *      json document
     * @return
     *      document identifier
     */
    public final String save(@NonNull JsonDocument doc) {
        String id = doc.getId();
        if (id == null || !exists(id)) {
            return collectionClient.insertOne(doc.getId(), doc.getData(), doc.getVector());
        }
        // Already Exist
        collectionClient.findOneAndReplace(UpdateQuery.builder()
                .where("_id")
                .isEqualsTo(id)
                .replaceBy(doc)
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
    public final List<String> saveAll(@NonNull List<Document<BEAN>> documentList) {
        if (documentList.isEmpty()) return new ArrayList<>();
        return documentList.stream().map(this::save).collect(Collectors.toList());
    }

    /**
     * Create a new document a generating identifier.
     *
     * @param documentList
     *      object Mapping
     * @return
     *      an unique identifier for the document
     */
    public final List<String> saveAllJsonDocuments(@NonNull List<JsonDocument> documentList) {
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
    public final List<String> insertAll(List<Document<BEAN>> documents) {
        if (documents == null || documents.isEmpty()) return new ArrayList<>();
        return collectionClient.insertMany(documents.stream()
                .map(Document::toJsonDocument)
                .collect(Collectors.toList()));
    }

    /**
     * Low level insertion of multiple records
     *
     * @param documentList
     *      list of documents
     * @return
     *      list of ids
     */
    public final List<String> insertAllJsonDocuments(@NonNull List<JsonDocument> documentList) {
        if (documentList.isEmpty()) return new ArrayList<>();
        return collectionClient.insertMany(documentList);
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
    public Optional<Result<BEAN>> findOne(@NonNull SelectQuery query) {
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
    public Optional<Result<BEAN>> findById(@NonNull String id) {
        return collectionClient.findById(id, docClass);
    }

    /**
     * Find all item in the collection.
     *
     * @return
     *      retrieve all items
     */
    public Stream<Result<BEAN>> findAll() {
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
    public Stream<Result<BEAN>> findAll(SelectQuery query) {
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
    public Page<Result<BEAN>> findPage(SelectQuery query) {
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
    public boolean delete(@NonNull Document<BEAN> document) {
        if (document.getId() != null) return deleteById(document.getId());
        if (document.getVector() != null) return collectionClient.deleteByVector(document.getVector()) > 0;
        throw new IllegalArgumentException("Cannot delete record without id or vector");
    }

    /**
     * Delete a document from id
     * .
     * @param id
     *      document id
     * @return
     *      if document has been deleted.
     */
    public boolean deleteById(String id) {
        return collectionClient.deleteById(id) > 0;
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
    public int deleteAll(List<Document<BEAN>> documents) {
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

}
