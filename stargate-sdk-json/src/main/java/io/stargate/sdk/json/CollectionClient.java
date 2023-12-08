package io.stargate.sdk.json;

import io.stargate.sdk.core.domain.ObjectMap;
import io.stargate.sdk.core.domain.Page;
import io.stargate.sdk.exception.AlreadyExistException;
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
import io.stargate.sdk.json.domain.SelectQueryBuilder;
import io.stargate.sdk.json.domain.UpdateQuery;
import io.stargate.sdk.json.domain.UpdateStatus;
import io.stargate.sdk.json.domain.odm.Document;
import io.stargate.sdk.json.domain.odm.Result;
import io.stargate.sdk.json.domain.odm.ResultMapper;
import io.stargate.sdk.json.exception.ApiException;
import io.stargate.sdk.utils.Assert;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
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
public class CollectionClient {

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
            ApiClient.rootResource.apply(node) + "/" + getNamespace() + "/" + getCollection();

    /**
     * Full constructor.
     *
     * @param httpClient client http
     * @param namespace namespace identifier
     * @param collection collection identifier
     */
    public CollectionClient(LoadBalancedHttpClient httpClient, String namespace, String collection) {
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
     * Insert with a Json Document.
     *
     * @param bean
     *      current bean
     * @param <DOC>
     *      type of object in use
     * @return
     *      new id
     */
    public final <DOC> String insertOne(@NonNull Document<DOC> bean) {
        return insertOne(bean.toJsonDocument());
    }

    /**
     * Insert a new document for a vector collection
     *
     * @param jsonDocument
     *      json Document
     * @return
     *      identifier for the document
     */
    public String insertOne(JsonDocument jsonDocument) {
        log.debug("insert into {}/{}", green(namespace), green(collection));
        return execute("insertOne", Map.of("document", jsonDocument))
                .getStatusKeyAsStringStream("insertedIds")
                .findAny()
                .orElse(null);
    }

    /**
     * Upsert a document in the collection.
     *
     * @param jsonDocument
     *      current document
     * @return
     *      document id
     */
    public String upsert(@NonNull JsonDocument jsonDocument) {
        if (jsonDocument.getId() == null) {
            jsonDocument.setId(UUID.randomUUID().toString());
        }
        try {
            insertOne(jsonDocument);
        } catch(ApiException e) {
            if ("DOCUMENT_ALREADY_EXISTS".equalsIgnoreCase(e.getErrorCode())) {
                // Already Exist
                findOneAndReplace(UpdateQuery.builder()
                        .where("_id")
                        .isEqualsTo(jsonDocument.getId())
                        .replaceBy(jsonDocument)
                        .build());
            } else {
                throw e;
            }
        }
        return jsonDocument.getId();
    }

    // --------------------------
    // ---   Insert Many     ----
    // --------------------------

    /**
     * Low level insertion of multiple records
     *
     * @param documents
     *      list of documents
     * @param <DOC>
     *      object T in use.
     * @return
     *      list of ids
     */
    public final <DOC> List<String> insertMany(List<DOC> documents) {
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
     * Check existence of a document from its id.
     * Projection to make it as light as possible.
     *
     * @param id
     *      document identifier
     * @return
     *      existence status
     */
    public boolean isDocumentExists(String id) {
        return findOne(SelectQuery.builder()
                .select("_id")
                .where("_id")
                .isEqualsTo(id).build()).isPresent();
    }

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
     * @param <DOC>
     *       class to be marshalled
     */
    public <DOC> Optional<Result<DOC>> findOne(SelectQuery query, Class<DOC> clazz) {
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
     * @param <DOC>
     *       class to be marshalled
     */
    public <DOC> Optional<Result<DOC>> findOne(SelectQuery query, ResultMapper<DOC> mapper) {
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
     * @param <DOC>
     *       class to be marshalled
     */
    public <DOC> Optional<Result<DOC>> findById(@NonNull String id, Class<DOC> clazz) {
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
     * @param <DOC>
     *       class to be marshalled
     */
    public <DOC> Optional<Result<DOC>> findById(@NonNull String id, ResultMapper<DOC> mapper) {
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
     * @param <DOC>
     *       class to be marshalled
     */
    public <DOC> Optional<Result<DOC>> findOneByVector(float[] vector, Class<DOC> clazz) {
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
     * @param <DOC>
     *       class to be marshalled
     */
    public <DOC> Optional<Result<DOC>> findOneByVector(float[] vector, ResultMapper<DOC> mapper) {
        return findOneByVector(vector).map(mapper::map);
    }

    // --------------------------
    // ---       Find        ----
    // --------------------------

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
     * @param <DOC>
     *       class to be marshalled
     */
    public  <DOC> Stream<Result<DOC>> query(SelectQuery pageQuery, Class<DOC> clazz) {
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
     * @param <DOC>
     *       class to be marshalled
     */
    public  <DOC> Stream<Result<DOC>> query(SelectQuery pageQuery, ResultMapper<DOC> mapper) {
        return query(pageQuery).map(mapper::map);
    }

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
     * Find All with Object Mapping.
     *
     * @param clazz
     *      class to be used
     * @return
     *      stream of results
     * @param <DOC>
     *       class to be marshalled
     */
    public <DOC> Stream<Result<DOC>> findAll(Class<DOC> clazz) {
        return findAll().map(r -> new Result<>(r, clazz));
    }

    /**
     * Find All with Object Mapping.
     *
     * @param mapper
     *      convert a json into expected pojo
     * @return
     *      stream of results
     * @param <DOC>
     *       class to be marshalled
     */
    public <DOC> Stream<Result<DOC>> findAll(ResultMapper<DOC> mapper) {
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
        return mapPageJsonResultAsPageResult(queryForPage(query), clazz);
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
     * @param <DOC>
     *     class to be marshalled
     */
    public <DOC> Page<Result<DOC>> queryForPage(SelectQuery query, ResultMapper<DOC> mapper) {
        return mapPageJsonResultAsPageResult(queryForPage(query), mapper);
    }

    /**
     * Map a page of JsonResult to Page of Result
     *
     * @param pageJson
     *      current page
     * @param clazz
     *      target beam
     * @param <DOC>
     *      type of object im page
     * @return
     *      new page
     */
    public <DOC> Page<Result<DOC>> mapPageJsonResultAsPageResult(Page<JsonResult> pageJson, Class<DOC> clazz) {
        return new Page<>(
                pageJson.getPageSize(),
                pageJson.getPageState().orElse(null),
                pageJson.getResults()
                        .stream()
                        .map(r -> new Result<>(r, clazz))
                        .collect(Collectors.toList()));
    }

    /**
     * Map a page of JsonResult to page result.
     *
     * @param pageJson
     *      current page
     * @param mapper
     *      mapper for the class
     * @param <DOC>
     *      type pf object im page
     * @return
     *      new page
     */
    public <DOC> Page<Result<DOC>> mapPageJsonResultAsPageResult(Page<JsonResult> pageJson, ResultMapper<DOC> mapper) {
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

    // ------------------------------
    // ---  Similarity Search    ----
    // ------------------------------

    /**
     * Query builder.
     *
     * @param vector
     *      vector embeddings
     * @param filter
     *      metadata filter
     * @param limit
     *      limit
     * @param pagingState
     *      paging state
     * @return
     *      result page
     */
    public Page<JsonResult> similaritySearch(float[] vector, Filter filter, Integer limit, String pagingState) {
        SelectQueryBuilder builder = SelectQuery.builder().orderByAnn(vector);
        if (filter != null) {
            if (builder.filter == null) {
                builder.filter = new HashMap<>();
            }
            builder.filter.putAll(filter.getFilter());
        }
        builder.includeSimilarity();
        if (pagingState != null) {
            builder.withPagingState(pagingState);
        }
        if (limit!=null) {
            builder.limit(limit);
        }
        return queryForPage(builder.build());
    }

    /**
     * Query builder.
     *
     * @param vector
     *      vector embeddings
     * @param limit
     *      limit for output
     * @return
     *      result page
     */
    public List<JsonResult> similaritySearch(float[] vector, Integer limit) {
        return similaritySearch(vector, null, limit, null).getResults();
    }

    /**
     * Query builder.
     *
     * @param vector
     *      vector embeddings
     * @param filter
     *      metadata filter
     * @param limit
     *      limit for output
     * @return
     *      result page
     */
    public List<JsonResult> similaritySearch(float[] vector, Filter filter, Integer limit) {
        return similaritySearch(vector, filter, limit, null).getResults();
    }

    /**
     * Search similarity from the vector (page by 20)
     *
     * @param vector
     *      vector embeddings
     * @param filter
     *      metadata filter
     * @param limit
     *      limit
     * @param pagingState
     *      paging state
     * @param clazz
     *      current class.
     * @param <DOC>
     *       type of document
     * @return
     *      page of results
     */
    public <DOC> Page<Result<DOC>> similaritySearch(float[] vector, Filter filter, Integer limit, String pagingState, Class<DOC> clazz) {
        return mapPageJsonResultAsPageResult(similaritySearch(vector, filter, limit, pagingState), clazz);
    }

    /**
     * Search similarity from the vector (page by 20)
     *
     * @param vector
     *      vector embeddings
     * @param filter
     *      metadata filter
     * @param limit
     *      limit
     * @param pagingState
     *      paging state
     * @param mapper
     *      result mapper
     * @param <DOC>
     *       type of document
     * @return
     *      page of results
     */
    public <DOC> Page<Result<DOC>> similaritySearch(float[] vector, Filter filter, Integer limit, String pagingState, ResultMapper<DOC> mapper) {
        return mapPageJsonResultAsPageResult(similaritySearch(vector, filter, limit, pagingState), mapper);
    }

    /**
     * Mapping method to have Json.
     *
     * @param res
     *      current result
     * @return
     *      list of document
     */
    private List<JsonResult> mapAsListJsonResult(List<Result<ObjectMap>> res) {
        return res.stream()
                .map(Result::toJsonResult)
                .collect(Collectors.toList());
    }

}
