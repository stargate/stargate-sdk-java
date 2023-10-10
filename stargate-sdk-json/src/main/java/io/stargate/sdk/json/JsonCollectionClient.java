package io.stargate.sdk.json;

import io.stargate.sdk.core.domain.Page;
import io.stargate.sdk.http.LoadBalancedHttpClient;
import io.stargate.sdk.http.ServiceHttp;
import io.stargate.sdk.json.domain.DeleteQuery;
import io.stargate.sdk.json.domain.JsonApiData;
import io.stargate.sdk.json.domain.JsonApiResponse;
import io.stargate.sdk.json.domain.Filter;
import io.stargate.sdk.json.domain.JsonRecord;
import io.stargate.sdk.json.domain.JsonResult;
import io.stargate.sdk.json.domain.JsonResultUpdate;
import io.stargate.sdk.json.domain.SelectQuery;
import io.stargate.sdk.json.domain.UpdateQuery;
import io.stargate.sdk.json.domain.UpdateStatus;
import io.stargate.sdk.json.domain.odm.Record;
import io.stargate.sdk.json.domain.odm.RecordMapper;
import io.stargate.sdk.utils.Assert;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.stargate.sdk.json.utils.JsonApiClientUtils.executeOperation;
import static io.stargate.sdk.utils.AnsiUtils.green;


/**
 * Wrapper for collection operations.
 */
@Slf4j
public class JsonCollectionClient {

    /** Get Topology of the nodes. */
    protected final LoadBalancedHttpClient stargateHttpClient;

    /** Namespace identifier. */
    private String namespace;

    /** Collection identifier. */
    private String collection;

    /**
     * Resource for collection: /v1/{namespace}/{collection}
     */
    public Function<ServiceHttp, String> collectionResource = (node) ->
            JsonApiClient.rootResource.apply(node) + "/" + namespace + "/" + collection;

    /**
     * Full constructor.
     *
     * @param httpClient client http
     * @param namespace namespace identifier
     * @param collection collection identifier
     */
    public JsonCollectionClient(LoadBalancedHttpClient httpClient, String namespace, String collection) {
        this.namespace          = namespace;
        this.collection         = collection;
        this.stargateHttpClient = httpClient;
        Assert.notNull(collection, "Collection");
        Assert.notNull(namespace, "Namespace");
    }

    // --------------------------
    // ---   Insert One      ----
    // --------------------------

    /**
     * Insert one Record.
     *
     * @param bean
     *      pojo to insert
     * @return
     *      record identifier
     */
    public String insertOne(Object bean) {
        return insertOne(null, bean, null);
    }

    /**
     * Insert one Record.
     *
     * @param id
     *      enforce identifier
     * @param bean
     *      pojo to insert
     * @return
     *      record identifier
     */
    public String insertOne(String id, Object bean) {
        return insertOne(id, bean, null);
    }

    /**
     * Insert one Record.
     *
     * @param bean
     *      pojo to insert
     * @param vector
     *      provide embeddings
     * @return
     *      record identifier
     */
    public String insertOne(Object bean, float[] vector) {
        return insertOne(null, bean, vector);
    }

    /**
     * Insert a new document for a vector collection
     *
     * @param id
     *      document identifier
     * @param bean
     *      object to insert
     * @param vector
     *      vector to be entered
     * @return
     *      identifier for the document
     */
    public String insertOne(String id, Object bean, float[] vector) {
        log.debug("insert into {}/{}", green(namespace), green(collection));
        return execute("insertOne", Map.of("document", new JsonRecord(id, bean, vector)))
                .getStatusKeyAsStream("insertedIds")
                .findAny()
                .orElse(null);
    }

    // --------------------------
    // ---   Insert Many     ----
    // --------------------------

    /**
     * Low level insertion of multiple records
     *
     * @param documents
     *      list of documents
     * @return
     *      list of ids
     */
    public final Stream<String> insertMany(JsonRecord... documents) {
        Objects.requireNonNull(documents, "documents");
        return insertMany(List.of(documents));
    }
    /**
     * Low level insertion of multiple records
     *
     * @param documents
     *      list of documents
     * @return
     *      list of ids
     */
    public final Stream<String> insertMany(List<JsonRecord> documents) {
        Objects.requireNonNull(documents, "documents");
        log.debug("insert into {}/{}", green(namespace), green(collection));
        return execute("insertMany", Map.of("documents", documents)).getStatusKeyAsStream("insertedIds");
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
    public Integer countDocuments() {
        return countDocuments(null);
    }

    /**
     * Count Document request.
     *
     * @return
     *      number of document.
     */
    public Integer countDocuments(Filter jsonFilter) {
        log.debug("Counting {}/{}", green(namespace), green(collection));
        return execute("countDocuments", jsonFilter).getStatusKeyAsInt("count");
    }

    // --------------------------
    // ---     Find One      ----
    // --------------------------

    /**
     * Find one document matching the query.
     *
     * @param query
     *      query documents and vector
     * @return
     *      result if exists
     */
    public Optional<JsonResult> findOne(SelectQuery query) {
        log.debug("Query in {}/{}", green(namespace), green(collection));
        return Optional.ofNullable(execute("findOne", query).getData().getDocument());
    }

    /**
     * Find one document matching the query.
     *
     * @param query
     *      query documents and vector
     * @param clazz
     *     class of the document
     * @return
     *      result if exists
     */
    public <T> Optional<Record<T>> findOne(SelectQuery query, Class<T> clazz) {
        return findOne(query).map(r -> new Record<>(r, clazz));
    }

    /**
     * Find one document matching the query.
     *
     * @param query
     *      query documents and vector
     * @return
     *      result if exists
     */
    public <T> Optional<Record<T>> findOne(SelectQuery query, RecordMapper<T> mapper) {
        return findOne(query).map(mapper::map);
    }

    // --------------------------
    // ---    Find By Id     ----
    // --------------------------

    public Optional<JsonResult> findById(String id) {
        return findOne(SelectQuery.findById(id));
    }

    public <T> Optional<Record<T>> findById(@NonNull String id, Class<T> clazz) {
        return findById(id).map(r -> new Record<>(r, clazz));
    }

    public <T> Optional<Record<T>> findById(@NonNull String id, RecordMapper<T> mapper) {
        return findById(id).map(mapper::map);
    }

    // --------------------------
    // --- Find By Vector    ----
    // --------------------------

    public Optional<JsonResult> findOneByVector(float[] vector) {
        return findOne(SelectQuery.findByVector(vector));
    }

    public <T> Optional<Record<T>> findOneByVector(float[] vector, Class<T> clazz) {
        return findOneByVector(vector).map(r -> new Record<>(r, clazz));
    }

    public <T> Optional<Record<T>> findOneByVector(float[] vector, RecordMapper<T> mapper) {
        return findOneByVector(vector).map(mapper::map);
    }

    // --------------------------
    // ---       Find        ----
    // --------------------------

    /**
     * Get all items in a collection.
     *
     * @return
     *      all items
     */
    public Stream<JsonResult> findAll() {
        return findAll(SelectQuery.builder().build());
    }

    /**
     * Get all items in a collection.
     * @param pageQuery
     *      filter
     * @return
     *      all items
     */
    public Stream<JsonResult> findAll(SelectQuery pageQuery) {
        List<JsonResult> documents = new ArrayList<>();
        String pageState = null;
        AtomicInteger pageCount = new AtomicInteger(0);
        do {
            log.debug("Fetching page "  + pageCount.incrementAndGet());
            Page<JsonResult> pageX = findPage(pageQuery);
            if (pageX.getPageState().isPresent())  {
                pageState = pageX.getPageState().get();
            } else {
                pageState = null;
            }
            documents.addAll(pageX.getResults());
            // Reusing query for next page
            pageQuery.setPageState(pageState);
        } while(pageState != null);
        return documents.stream();
    }

    public  <T> Stream<Record<T>>  findAll(SelectQuery pageQuery, Class<T> clazz) {
        return findAll(pageQuery).map(r -> new Record<>(r, clazz));
    }

    public  <T> Stream<Record<T>>  findAll(SelectQuery pageQuery, RecordMapper<T> mapper) {
        return findAll(pageQuery).map(mapper::map);
    }

    /**
     * Find All with Object Mapping.
     *
     * @param clazz
     *      class to be used
     * @return
     *      stream of results
     * @param <T>
     *       class to be marshalled
     */
    public <T> Stream<Record<T>> findAll(Class<T> clazz) {
        return findAll().map(r -> new Record<>(r, clazz));
    }

    public <T> Stream<Record<T>> findAll(RecordMapper<T> mapper) {
        return findAll().map(mapper::map);
    }

    /**
     * Find documents matching the query.
     *
     * @param query
     *      current query
     * @return
     *      page of results
     */
    public Page<JsonResult> findPage(SelectQuery query) {
        log.debug("Query in {}/{}", green(namespace), green(collection));
        JsonApiData apiData = execute("find", query).getData();
        int pageSize = (query != null) ? query.getPageSize() : SelectQuery.DEFAULT_PAGE_SIZE;
        return new Page<>(pageSize, apiData.getNextPageState(), apiData.getDocuments());
    }

    public <T> Page<Record<T>> findPage(SelectQuery query, Class<T> clazz) {
        Page<JsonResult> pageJson = findPage(query);
        return new Page<>(
                pageJson.getPageSize(),
                pageJson.getPageState().orElse(null),
                pageJson.getResults()
                        .stream()
                        .map(r -> new Record<>(r, clazz))
                        .collect(Collectors.toList()));
    }

    private <T> Page<Record<T>> findPage(SelectQuery query, RecordMapper<T> mapper) {
        Page<JsonResult> pageJson = findPage(query);
        return new Page<>(
                pageJson.getPageSize(),
                pageJson.getPageState().orElse(null),
                pageJson.getResults().stream()
                        .map(mapper::map)
                        .collect(Collectors.toList()));
    }

    // --------------------------
    // ---     Delete One    ----
    // --------------------------

    public int deleteOne(DeleteQuery deleteQuery) {
        log.debug("Delete in {}/{}", green(namespace), green(collection));
        return execute("deleteOne", deleteQuery).getStatusKeyAsInt("deletedCount");
    }

    public int deleteById(String id) {
        return deleteOne(DeleteQuery.deleteById(id));
    }

    public int deleteByVector(float[] vector) {
        return deleteOne(DeleteQuery.deleteByVector(vector));
    }

    // --------------------------
    // ---     Delete Many   ----
    // --------------------------

    public int deleteMany(DeleteQuery deleteQuery) {
        log.debug("Delete in {}/{}", green(namespace), green(collection));
        return execute("deleteMany", deleteQuery).getStatusKeyAsInt("deletedCount");
    }

    public int deleteAll() {
        return deleteMany(null);
    }

    // --------------------------
    // ---  Update           ----
    // --------------------------

    public JsonResultUpdate findOneAndUpdate(UpdateQuery query) {
        return updateQuery("findOneAndUpdate", query);
    }

    public JsonResultUpdate findOneAndReplace(UpdateQuery query) {
        return updateQuery("findOneAndReplace", query);
    }

    public JsonResultUpdate findOneAndDelete(UpdateQuery query) {
        return updateQuery("findOneAndDelete", query);
    }

    private JsonResultUpdate updateQuery(String operation, UpdateQuery query) {
        log.debug("{} in {}/{}", operation, green(namespace), green(collection));
        JsonApiResponse response = execute(operation, query);
        JsonResultUpdate jru = new JsonResultUpdate();
        if (response.getData() != null) {
            jru.setJsonResult(response.getData().getDocument());
        }
        if (response.getStatus() != null) {
            jru.setUpdateStatus(buildUpdateStatus(response.getStatus()));
        }
        return jru;
    }

    private UpdateStatus buildUpdateStatus(Map<String, Object> status) {
        UpdateStatus updateStatus = new UpdateStatus();
        status.computeIfPresent("upsertedId", (k, v) -> {
            updateStatus.setUpsertedId(v.toString());
            return v;
        });
        status.computeIfPresent("modifiedCount", (k, v) -> {
            updateStatus.setModifiedCount((Integer) v);
            return v;
        });
        status.computeIfPresent("matchedCount", (k, v) -> {
            updateStatus.setMatchedCount((Integer) v);
            return v;
        });
        status.computeIfPresent("deletedCount", (k, v) -> {
            updateStatus.setDeletedCount((Integer) v);
            return v;
        });
        return updateStatus;
    }

    // --------------------------
    // ---  UpdateOne        ----
    // --------------------------

    public UpdateStatus updateOne(UpdateQuery query) {
        log.debug("updateOne in {}/{}", green(namespace), green(collection));
        return updateQuery("updateOne", query).getUpdateStatus();
    }

    // --------------------------
    // ---  UpdateMany       ----
    // --------------------------

    public UpdateStatus updateMany(UpdateQuery query) {
        log.debug("updateMany in {}/{}", green(namespace), green(collection));
        return updateQuery("updateMany", query).getUpdateStatus();
    }

    /**
     * Syntax sugar.
     *
     * @param operation
     *      operation to run
     * @param payload
     *      payload returned
     */
    private JsonApiResponse execute(String operation, Object payload) {
        return executeOperation(stargateHttpClient, collectionResource, operation, payload);
    }

}
