package io.stargate.sdk.json;

import io.stargate.sdk.core.domain.Page;
import io.stargate.sdk.http.LoadBalancedHttpClient;
import io.stargate.sdk.http.ServiceHttp;
import io.stargate.sdk.json.domain.DeleteQuery;
import io.stargate.sdk.json.domain.Filter;
import io.stargate.sdk.json.domain.JsonApiData;
import io.stargate.sdk.json.domain.JsonApiResponse;
import io.stargate.sdk.json.domain.JsonDocument;
import io.stargate.sdk.json.domain.JsonResult;
import io.stargate.sdk.json.domain.JsonResultUpdate;
import io.stargate.sdk.json.domain.SelectQuery;
import io.stargate.sdk.json.domain.UpdateQuery;
import io.stargate.sdk.json.domain.UpdateStatus;
import io.stargate.sdk.json.domain.odm.Document;
import io.stargate.sdk.json.domain.odm.Result;
import io.stargate.sdk.json.domain.odm.ResultMapper;
import io.stargate.sdk.utils.Assert;
import lombok.Getter;
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
    @Getter
    private final String namespace;

    /** Collection identifier. */
    @Getter
    private final String collection;

    /**
     * Resource for collection: /v1/{namespace}/{collection}
     */
    public Function<ServiceHttp, String> collectionResource = (node) ->
            JsonApiClient.rootResource.apply(node) + "/" + getNamespace() + "/" + getCollection();

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
        JsonDocument jsonDocument = new JsonDocument(id, bean, vector);
        if (bean instanceof JsonDocument) {
            jsonDocument = (JsonDocument) bean;
            if (id != null) {
                jsonDocument.id(id);
            }
            if (vector != null) {
                jsonDocument.vector(vector);
            }
        }
        if (bean instanceof Document) {
            jsonDocument = ((Document<?>) bean).toJsonDocument();
            if (id != null) {
                jsonDocument.id(id);
            }
            if (vector != null) {
                jsonDocument.vector(vector);
            }
        }
        return execute("insertOne", Map.of("document", jsonDocument))
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
    public final List<String> insertMany(List<JsonDocument> documents) {
        Objects.requireNonNull(documents, "documents");
        log.debug("insert into {}/{}", green(namespace), green(collection));
        return execute("insertMany", Map.of("documents", documents)).getStatusKeyAsList("insertedIds");
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
     * @param jsonFilter
     *      request to filter for count
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
     * @param <T>
     *       class to be marshalled
     */
    public <T> Optional<Result<T>> findOne(SelectQuery query, Class<T> clazz) {
        return findOne(query).map(r -> new Result<>(r, clazz));
    }

    /**
     * Find one document matching the query.
     *
     * @param query
     *      query documents and vector
     * @param mapper
     *      convert a json into expected pojo
     * @return
     *      result if exists
     * @param <T>
     *       class to be marshalled
     */
    public <T> Optional<Result<T>> findOne(SelectQuery query, ResultMapper<T> mapper) {
        return findOne(query).map(mapper::map);
    }

    // --------------------------
    // ---    Find By Id     ----
    // --------------------------

    /**
     * Find document from its id.
     *
     * @param id
     *      document identifier
     * @return
     *      document
     */
    public Optional<JsonResult> findById(String id) {
        return findOne(SelectQuery.findById(id));
    }

    /**
     * Find document from its id.
     *
     * @param id
     *      document identifier
     * @param clazz
     *      class for target pojo
     * @return
     *      document
     * @param <T>
     *       class to be marshalled
     */
    public <T> Optional<Result<T>> findById(@NonNull String id, Class<T> clazz) {
        return findById(id).map(r -> new Result<>(r, clazz));
    }

    /**
     * Find document from its id.
     *
     * @param id
     *      document identifier
     * @param mapper
     *      convert a json into expected pojo
     * @return
     *      document
     * @param <T>
     *       class to be marshalled
     */
    public <T> Optional<Result<T>> findById(@NonNull String id, ResultMapper<T> mapper) {
        return findById(id).map(mapper::map);
    }

    // --------------------------
    // --- Find By Vector    ----
    // --------------------------


    /**
     * Find document from its vector.
     *
     * @param vector
     *      document vector
     * @return
     *      document
     */
    public Optional<JsonResult> findOneByVector(float[] vector) {
        return findOne(SelectQuery.findByVector(vector));
    }

    /**
     * Find document from its vector.
     *
     * @param vector
     *      document vector
     * @param clazz
     *      class for target pojo
     * @return
     *      document
     * @param <T>
     *       class to be marshalled
     */
    public <T> Optional<Result<T>> findOneByVector(float[] vector, Class<T> clazz) {
        return findOneByVector(vector).map(r -> new Result<>(r, clazz));
    }

    /**
     * Find document from its vector.
     *
     * @param vector
     *      document vector
     * @param mapper
     *      convert a json into expected pojo
     * @return
     *      document
     * @param <T>
     *       class to be marshalled
     */
    public <T> Optional<Result<T>> findOneByVector(float[] vector, ResultMapper<T> mapper) {
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
        return query(SelectQuery.builder().build());
    }

    /**
     * Search records with a filter
     *
     * @param pageQuery
     *      filter
     * @return
     *      all items
     */
    public Stream<JsonResult> query(SelectQuery pageQuery) {
        List<JsonResult> documents = new ArrayList<>();
        String pageState = null;
        AtomicInteger pageCount = new AtomicInteger(0);
        do {
            log.debug("Fetching page "  + pageCount.incrementAndGet());
            Page<JsonResult> pageX = queryForPage(pageQuery);
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

    /**
     * Search records with a filter
     *
     * @param pageQuery
     *      filter
     * @param clazz
     *      class for target pojo
     * @return
     *      all items
     * @param <T>
     *       class to be marshalled
     */
    public  <T> Stream<Result<T>> query(SelectQuery pageQuery, Class<T> clazz) {
        return query(pageQuery).map(r -> new Result<>(r, clazz));
    }

    /**
     * Search records with a filter
     *
     * @param pageQuery
     *      filter
     * @param mapper
     *      convert a json into expected pojo
     * @return
     *      all items
     * @param <T>
     *       class to be marshalled
     */
    public  <T> Stream<Result<T>> query(SelectQuery pageQuery, ResultMapper<T> mapper) {
        return query(pageQuery).map(mapper::map);
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
    public <T> Stream<Result<T>> findAll(Class<T> clazz) {
        return findAll().map(r -> new Result<>(r, clazz));
    }

    /**
     * Find All with Object Mapping.
     *
     * @param mapper
     *      convert a json into expected pojo
     * @return
     *      stream of results
     * @param <T>
     *       class to be marshalled
     */
    public <T> Stream<Result<T>> findAll(ResultMapper<T> mapper) {
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
    public Page<JsonResult> queryForPage(SelectQuery query) {
        log.debug("Query in {}/{}", green(namespace), green(collection));
        JsonApiData apiData = execute("find", query).getData();
        int pageSize = (query != null) ? query.getPageSize() : SelectQuery.DEFAULT_PAGE_SIZE;
        return new Page<>(pageSize, apiData.getNextPageState(), apiData.getDocuments());
    }


    /**
     * Find documents matching the query.
     *
     * @param query
     *      current query
     * @param clazz
     *      class for target pojo
     * @return
     *      page of results
     * @param <T>
     *     class to be marshalled
     */
    public <T> Page<Result<T>> queryForPage(SelectQuery query, Class<T> clazz) {
        Page<JsonResult> pageJson = queryForPage(query);
        return new Page<>(
                pageJson.getPageSize(),
                pageJson.getPageState().orElse(null),
                pageJson.getResults()
                        .stream()
                        .map(r -> new Result<>(r, clazz))
                        .collect(Collectors.toList()));
    }

    /**
     * Find documents matching the query.
     *
     * @param query
     *      current query
     * @param mapper
     *      mapper to convert into target pojo
     * @return
     *      page of results
     * @param <T>
     *     class to be marshalled
     */
    private <T> Page<Result<T>> queryForPage(SelectQuery query, ResultMapper<T> mapper) {
        Page<JsonResult> pageJson = queryForPage(query);
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

    /**
     * Delete single record from a request.
     *
     * @param deleteQuery
     *      delete query
     * @return
     *      number of deleted records
     */
    public int deleteOne(DeleteQuery deleteQuery) {
        log.debug("Delete in {}/{}", green(namespace), green(collection));
        return execute("deleteOne", deleteQuery).getStatusKeyAsInt("deletedCount");
    }

    /**
     * Delete single record from its id.
     *
     * @param id
     *      id
     * @return
     *      number of deleted records
     */
    public int deleteById(String id) {
        return deleteOne(DeleteQuery.deleteById(id));
    }

    /**
     * Delete single record from its vector.
     *
     * @param vector
     *      vector
     * @return
     *      number of deleted records
     */
    public int deleteByVector(float[] vector) {
        return deleteOne(DeleteQuery.deleteByVector(vector));
    }

    // --------------------------
    // ---     Delete Many   ----
    // --------------------------

    /**
     * Delete multiple records from a request.
     *
     * @param deleteQuery
     *      delete query
     * @return
     *      number of deleted records
     */
    public int deleteMany(DeleteQuery deleteQuery) {
        log.debug("Delete in {}/{}", green(namespace), green(collection));
        return execute("deleteMany", deleteQuery).getStatusKeyAsInt("deletedCount");
    }


    /**
     * Clear the collection.
     *
     * @return
     *      number of items deleted
     */
    public int deleteAll() {
        return deleteMany(null);
    }

    // --------------------------
    // ---  Update           ----
    // --------------------------

    /**
     * Find ana update a record based on a query,
     *
     * @param query
     *      query to find the record
     * @return
     *      result of the update
     */
    public JsonResultUpdate findOneAndUpdate(UpdateQuery query) {
        return updateQuery("findOneAndUpdate", query);
    }

    /**
     * Find ana replace a record based on a query,
     *
     * @param query
     *      query to find the record
     * @return
     *      result of the update
     */
    public JsonResultUpdate findOneAndReplace(UpdateQuery query) {
        return updateQuery("findOneAndReplace", query);
    }

    /**
     * Find ana delete a record based on a query.
     *
     * @param query
     *      query to find the record
     * @return
     *      result of the update
     */
    public JsonResultUpdate findOneAndDelete(UpdateQuery query) {
        return updateQuery("findOneAndDelete", query);
    }

    /**
     * Utility o build the delete query.
     *
     * @param operation
     *      operation to used
     * @param query
     *      uquery to use
     * @return
     *      returned object by the Api
     */
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


    /**
     * Utility to parse the status in the response.
     *
     * @param status
     *      map status in the api response
     * @return
     *      object marshalled
     */
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

    /**
     * Update a single record.
     *
     * @param query
     *      query to find the record
     * @return
     *      update status
     */
    public UpdateStatus updateOne(UpdateQuery query) {
        log.debug("updateOne in {}/{}", green(namespace), green(collection));
        return updateQuery("updateOne", query).getUpdateStatus();
    }

    // --------------------------
    // ---  UpdateMany       ----
    // --------------------------

    /**
     * Update many records.
     *
     * @param query
     *      query to find the record
     * @return
     *      update status
     */
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
