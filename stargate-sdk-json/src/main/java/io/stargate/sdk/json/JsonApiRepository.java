package io.stargate.sdk.json;

import io.stargate.sdk.core.domain.Page;
import io.stargate.sdk.json.domain.DeleteQuery;
import io.stargate.sdk.json.domain.Filter;
import io.stargate.sdk.json.domain.JsonRecord;
import io.stargate.sdk.json.domain.JsonResultUpdate;
import io.stargate.sdk.json.domain.SelectQuery;
import io.stargate.sdk.json.domain.UpdateQuery;
import io.stargate.sdk.json.domain.odm.Record;
import lombok.NonNull;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Crud repository with Json Api
 *
 * @param <BEAN>
 */
public class JsonApiRepository<BEAN> {

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
    public JsonApiRepository(JsonCollectionClient col, Class<BEAN> clazz) {
        this.collectionClient = col;
        this.docClass  = clazz;
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
    // ---        Save       ----
    // --------------------------

    /**
     * Create a new document a generating identifier.
     * @param current
     *      delete a document
     * @return
     *      generated id
     */
    public final String create(@NonNull BEAN current) {
        return collectionClient.insertOne(current, null);
    }

    /**
     * Create a new document with the id, if I already exist an error will occur.
     *
     * @param current
     *      document to create
     * @return
     *      generated id
     */
    public final String create( @NonNull String id, @NonNull BEAN current) {
        return collectionClient.insertOne(id, current, null);
    }

    /**
     * Generate a new document with a new id.
     *
     * @param current
     *      current object to create
     * @return
     *      generated id
     */
    public final String save(@NonNull BEAN current) {
        return create(current);
    }

    /**
     * Create a new document with the id, if I already exist an error will occur.
     * @param id
     *      identifier
     * @param current
     *      current bean
     * @return
     *      if the document has been updated
     */
    public boolean save(String id, @NonNull BEAN current) {
        if (!exists(id)) {
            create(id, current);
            return true;
        }
        JsonResultUpdate res = collectionClient
                .findOneAndReplace(UpdateQuery.builder()
                .where("_id")
                .isEqualsTo(id)
                .replaceBy(new JsonRecord(id, current))
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
    public final String save(@NonNull Record<BEAN> current) {
        if (current.getId() != null && !exists(current.getId())) {
            create(current.getId(), current.getData());
        }


        return collectionClient.insertOne(current.asJsonRecord());
    }

    /**
     * Create a new document a generating identifier.
     *
     * @param current
     *      object Mapping
     * @return
     *      an unique identifier for the document
     */
    public final int saveAll(@NonNull Record<BEAN> current) {
        //if (current.getId() != null) return save(current.getId(), current.getData());
        //return collectionClient.insertOne(current.asJsonRecord());
        return 0;
    }

    /**
     * Low level insertion of multiple records
     *
     * @param records
     *      list of records
     * @return
     *      list of ids
     */
    @SafeVarargs
    public final Stream<String> createAll(Record<BEAN>... records) {
        if (records == null || records.length == 0) return Stream.empty();
        return createAll(Arrays.asList(records));
    }

    /**
     * Low level insertion of multiple records
     *
     * @param documents
     *      list of documents
     * @return
     *      list of ids
     */
    public final Stream<String> createAll(List<Record<BEAN>> documents) {
        if (documents == null || documents.isEmpty()) return Stream.empty();
        return collectionClient.insertMany(documents.stream()
                .map(Record::asJsonRecord)
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
    public Optional<Record<BEAN>> findOne(@NonNull SelectQuery query) {
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
    public Optional<Record<BEAN>> findById(@NonNull String id) {
        return collectionClient.findById(id, docClass);
    }

    /**
     * Find all item in the collection.
     *
     * @return
     *      retrieve all items
     */
    public Stream<Record<BEAN>> findAll() {
        return collectionClient.findAll(docClass);
    }

    /**
     * Find all item in the collection.
     *
     * @return
     *      retrieve all items
     */
    public Stream<Record<BEAN>> findAll(SelectQuery query) {
        return collectionClient.findAll(query, docClass);
    }

    /**
     * Find a page in the collection.
     *
     * @param query
     *      current query
     * @return
     *      page of records
     */
    public Page<Record<BEAN>> findPage(SelectQuery query) {
        return collectionClient.findPage(query, docClass);
    }

    // --------------------------
    // ---     Delete        ----
    // --------------------------

    public boolean delete(@NonNull Record<BEAN> record) {
        if (record.getId() != null) return deleteById(record.getId());
        if (record.getVector() != null) return collectionClient.deleteByVector(record.getVector()) > 0;
        throw new IllegalArgumentException("Cannot delete record without id or vector");
    }

    public boolean deleteById(String id) {
        return collectionClient.deleteById(id) > 0;
    }

    public int deleteAll() {
        return collectionClient.deleteAll();
    }

    /**
     * Use parallelism and async to delete all records.
     *
     * @param records
     *      list of records
     * @return
     *      number of records deleted
     */
    public int deleteAll(List<Record<BEAN>> records) {
        List<CompletableFuture<Integer>> futures = records.stream()
                .map(record -> CompletableFuture.supplyAsync(() -> delete(record) ? 1 : 0))
                .collect(Collectors.toList());
        return futures.stream()
                .map(CompletableFuture::join) // This will wait for the result of each future
                .mapToInt(Integer::intValue)
                .sum();
    }

    public int deleteAll(DeleteQuery deleteQuery) {
        return collectionClient.deleteMany(deleteQuery);
    }

}
