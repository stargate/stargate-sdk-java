package io.stargate.sdk.data;

import io.stargate.sdk.core.domain.Page;
import io.stargate.sdk.data.domain.ApiData;
import io.stargate.sdk.data.domain.ApiError;
import io.stargate.sdk.data.domain.ApiResponse;
import io.stargate.sdk.data.domain.DocumentMutationResult;
import io.stargate.sdk.data.domain.DocumentMutationStatus;
import io.stargate.sdk.data.domain.JsonDocument;
import io.stargate.sdk.data.domain.JsonDocumentMutationResult;
import io.stargate.sdk.data.domain.JsonDocumentResult;
import io.stargate.sdk.data.domain.JsonResultUpdate;
import io.stargate.sdk.data.domain.UpdateStatus;
import io.stargate.sdk.data.domain.odm.Document;
import io.stargate.sdk.data.domain.odm.DocumentResult;
import io.stargate.sdk.data.domain.odm.DocumentResultMapper;
import io.stargate.sdk.data.domain.query.DeleteQuery;
import io.stargate.sdk.data.domain.query.DeleteResult;
import io.stargate.sdk.data.domain.query.Filter;
import io.stargate.sdk.data.domain.query.SelectQuery;
import io.stargate.sdk.data.domain.query.UpdateQuery;
import io.stargate.sdk.data.exception.DataApiDocumentAlreadyExistException;
import io.stargate.sdk.http.ServiceHttp;
import io.stargate.sdk.utils.Assert;
import io.stargate.sdk.utils.JsonUtils;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.stargate.sdk.data.utils.DataApiUtils.executeOperation;
import static io.stargate.sdk.data.utils.DataApiUtils.validate;
import static io.stargate.sdk.utils.AnsiUtils.green;

/**
 * Client for a collection (crud for documents).
 */
@Slf4j
public class CollectionClient {

    /** Collection identifier. */
    @Getter
    private final String collection;

    /** keep reference to namespace client. */
    private final NamespaceClient namespaceClient;

    /** Resource collection. */
    public final Function<ServiceHttp, String> collectionResource;

    /** Flag to enforce the order of insertion. */
    @Getter @Setter
    private boolean insertManyOrdered = false;

    /**
     * Full constructor.
     *
     * @param namespaceClient
     *      client namespace http
     * @param collection
     *      collection identifier
     */
    protected CollectionClient(@NonNull NamespaceClient namespaceClient, @NonNull String collection) {
        this.collection         = collection;
        this.namespaceClient    = namespaceClient;
        this.collectionResource = (node) -> namespaceClient.getNamespaceResource().apply(node) + "/" + getCollection();
    }

    // --------------------------
    // ---   Insert One      ----
    // --------------------------

    /**
     * Insert with a Json Document.
     *
     * @param json
     *     json String
     * @return
     *      document identifier and status
     */
    public final JsonDocumentMutationResult insertOne(String json) {
        Assert.hasLength(json, "Json input");
        return insertOne(new JsonDocument(json));
    }

    /**
     * Insert with a Json Document asynchronously
     *
     * @param json
     *     json String
     * @return
     *      document identifier and status
     */
    public final CompletableFuture<JsonDocumentMutationResult> insertOneAsync(String json) {
        return CompletableFuture.supplyAsync(() -> insertOne(json));
    }

    /**
     * Insert with a Json Document (schemaless)
     *
     * @param document
     *      current bean
     * @return
     *      mutation result with status and id
     */
    public final JsonDocumentMutationResult insertOne(@NonNull JsonDocument document) {
        // Enforce call to other methods
        DocumentMutationResult<Map<String, Object>> mutationResult = insertOne((Document<Map<String, Object>>) document);
        // Mapping of the output
        JsonDocumentMutationResult res = new JsonDocumentMutationResult();
        res.setStatus(mutationResult.getStatus());
        res.setDocument(mutationResult.getDocument());
        return res;
    }

    /**
     * Insert with a Json Document (schemaless)
     *
     * @param document
     *      current bean
     * @return
     *      mutation result with status and id
     */
    public final CompletableFuture<JsonDocumentMutationResult> insertOneAsync(@NonNull JsonDocument document) {
        return CompletableFuture.supplyAsync(() -> insertOne(document));
    }

    /**
     * Insert with a Json Document.
     *
     * @param document
     *      current document
     * @return
     *      document identifier and status
     * @param <T>
     *     represent the pojo, payload of document
     */
    public final <T> DocumentMutationResult<T> insertOne(@NonNull Document<T> document) {
        log.debug("insert into {}/{}", green(namespaceClient.getNamespace()), green(collection));
        if (document.getId() == null) {
            // Enforce the UUID at client side to retrieve it in an easier way
            document.setId(UUID.randomUUID().toString());
        }
        ApiResponse response = execute("insertOne", Map.of("document", document));
        if (response.getErrors()!= null && !response.getErrors().isEmpty()) {
            throw new DataApiDocumentAlreadyExistException(response.getErrors().get(0));
        }
        return new DocumentMutationResult<>(document, DocumentMutationStatus.CREATED);
    }

    /**
     * Insert with a Json Document.
     *
     * @param document
     *      current document
     * @param <T>
     *     represent the pojo, payload of document
     * @return
     *      document identifier and status
     */
    public final <T> CompletableFuture<DocumentMutationResult<T>> insertOneASync(@NonNull Document<T> document) {
        return CompletableFuture.supplyAsync(() -> insertOne(document));
    }

    // --------------------------
    // ---   Upsert One      ----
    // --------------------------

    /**
     * Upsert a document in the collection.
     *
     * @param json
     *      json to insert
     * @return
     *      document status and identifier
     */
    public final JsonDocumentMutationResult upsertOne(String json) {
        Assert.hasLength(json, "Json input");
        return upsertOne(new JsonDocument(json));
    }

    /**
     * Upsert a document in the collection.
     *
     * @param json
     *      json to insert
     * @return
     *      document status and identifier
     */
    public final CompletableFuture<JsonDocumentMutationResult> upsertOneAsync(String json) {
        return CompletableFuture.supplyAsync(() -> upsertOne(json));
    }

    /**
     * Upsert a document in the collection.
     *
     * @param document
     *      document to insert
     * @return
     *      document status and identifier
     */
    public final JsonDocumentMutationResult upsertOne(@NonNull JsonDocument document) {
        // Enforce call to other methods
        DocumentMutationResult<Map<String, Object>> mutationResult = upsertOne((Document<Map<String, Object>>) document);
        // Mapping of the output
        JsonDocumentMutationResult res = new JsonDocumentMutationResult();
        res.setStatus(mutationResult.getStatus());
        res.setDocument(mutationResult.getDocument());
        return res;
    }

    /**
     * Upsert a document in the collection.
     *
     * @param document
     *      json Document to insert
     * @return
     *      document status and identifier
     */
    public final CompletableFuture<JsonDocumentMutationResult> upsertOneAsync(@NonNull JsonDocument document) {
        return CompletableFuture.supplyAsync(() -> upsertOne(document));
    }

    /**
     * Upsert a document in the collection.
     *
     * @param document
     *      document to insert
     * @param <DOC>
     *     represent the pojo, payload of document
     * @return
     *      document status and identifier
     */
    public <DOC> DocumentMutationResult<DOC> upsertOne(@NonNull Document<DOC> document) {
        log.debug("upsert into {}/{}", green(namespaceClient.getNamespace()), green(collection));
        if (document.getId() == null) {
            document.setId(UUID.randomUUID().toString());
       }
       JsonResultUpdate u = findOneAndReplace(UpdateQuery.builder()
               .filter(new Filter().where("_id").isEqualsTo(document.getId()))
               .replaceBy(document)
               .withUpsert() // with option upsert=true
               .build());
        DocumentMutationResult<DOC> result = new DocumentMutationResult<>(document);
        if (u.getUpdateStatus().getUpsertedId() != null && u.getUpdateStatus().getUpsertedId().equals(document.getId())) {
            result.setStatus(DocumentMutationStatus.CREATED);
        } else if (u.getUpdateStatus().getModifiedCount() == 0) {
            result.setStatus(DocumentMutationStatus.UNCHANGED);
        } else {
            result.setStatus(DocumentMutationStatus.UPDATED);
        }
        return result;
    }

    /**
     * Upsert with Asynchronous method.
     *
     * @param document
     *      document to insert
     * @return
     *      completion future
     * @param <DOC>
     *      current document nature
     */
    public <DOC> CompletableFuture<DocumentMutationResult<DOC>> upsertOneASync(@NonNull Document<DOC> document) {
        return CompletableFuture.supplyAsync(() -> upsertOne(document));
    }

    // --------------------------
    // ---   Insert Many     ----
    // --------------------------

    /**
     * Try Insert Many with a String
     *
     * @param json
     *      current Json
     * @return
     *      list of status
     */
    public final List<JsonDocumentMutationResult> insertMany(String json) {
        return insertManyJsonDocuments(mapJsonStringToJsonDocumentList(json));
    }

    /**
     * Try Insert Many with a String Asynchronously.
     *
     * @param json
     *      current Json
     * @return
     *      list of status
     */
    public final CompletableFuture<List<JsonDocumentMutationResult>> insertManyASync(String json) {
        return CompletableFuture.supplyAsync(() -> insertMany(json));
    }

    /**
     * Low level insertion of multiple records
     *
     * @param documents
     *      list of documents
     * @return
     *      list of ids
     */
    public final List<JsonDocumentMutationResult> insertManyJsonDocuments(List<JsonDocument> documents) {
        return mapJsonDocumentMutationResultList(insertMany(mapJsonDocumentList(documents)));
    }

    /**
     * Insert Asynchronously a list of documents.
     *
     * @param documents
     *      document list
     * @return
     *      list of statuses when complete.
     */
    public final CompletableFuture<List<JsonDocumentMutationResult>> insertManyJsonDocumentsASync(List<JsonDocument> documents) {
        return CompletableFuture.supplyAsync(() -> insertManyJsonDocuments(documents));
    }

    /**
     * Insert a list of JsonDocument.
     *
     * @param documents
     *      document list
     * @return
     *      list of statuses when complete.
     */
    public final List<JsonDocumentMutationResult> insertMany(JsonDocument... documents) {
        return insertManyJsonDocuments(Arrays.asList(documents));
    }

    /**
     * Insert Asynchronously a list of documents.
     *
     * @param documents
     *      document list
     * @return
     *      list of statuses when complete.
     */
    public final CompletableFuture<List<JsonDocumentMutationResult>> insertManyAsync(JsonDocument... documents) {
        return insertManyJsonDocumentsASync(Arrays.asList(documents));
    }

    /**
     * Insert Documents: Default is non ordered and no replace.
     *
     * @param documents
     *      list of documents
     * @param <DOC>
     *     represent the pojo, payload of document
     * @return
     *      list of ids
     */
    public final <DOC> List<DocumentMutationResult<DOC>> insertMany(List<Document<DOC>> documents) {
        return insertMany(documents, false);
    }

    /**
     * Try Insert Many with a String Asynchronously.
     *
     * @param documents
     *      list of documents
     * @param <DOC>
     *     represent the pojo, payload of document
     * @return
     *      list of status
     */
    public final <DOC> CompletableFuture<List<DocumentMutationResult<DOC>>> insertManyASync(List<Document<DOC>> documents) {
        return CompletableFuture.supplyAsync(() -> insertMany(documents));
    }

    /**
     * Insert a list of documents.
     *
     * @param documents
     *      document list
     * @return
     *      list of statuses when complete.
     * @param <DOC>
     *     represent the pojo, payload of document
     */
    @SafeVarargs
    public final <DOC> List<DocumentMutationResult<DOC>> insertMany(Document<DOC>... documents) {
        return insertMany(Arrays.asList(documents));
    }

    /**
     * Insert Asynchronously a list of documents.
     *
     * @param documents
     *      document list
     * @param <DOC>
     *     represent the pojo, payload of document
     * @return
     *      list of statuses when complete.
     */
    @SafeVarargs
    public final  <DOC> CompletableFuture<List<DocumentMutationResult<DOC>>> insertManyAsync(Document<DOC>... documents) {
        return insertManyASync(Arrays.asList(documents));
    }

    // ------------------------------
    // ---      Upsert Many      ----
    // ------------------------------

    /**
     * Upsert of up to 20 documents, expressed as an array within json String.
     *
     * @param json
     *      an array of documents within json String.
     * @return
     *      insertion status for each document (in order of input).
     */
    public final List<JsonDocumentMutationResult> upsertMany(String json) {
        return upsertManyJsonDocuments(mapJsonStringToJsonDocumentList(json));
    }

    /**
     * Upsert of up to 20 documents Asynchronously, expressed as an array within json String.
     *
     * @param json
     *      an array of documents within json String.
     * @return
     *      insertion status for each document (in order of input).
     */
    public final CompletableFuture<List<JsonDocumentMutationResult>> upsertManyASync(String json) {
        return CompletableFuture.supplyAsync(() -> upsertMany(json));
    }

    /**
     * Upsert of up to 20 documents, expressed as key/value documents.
     *
     * @param documents
     *      list of documents
     * @return
     *      list of ids
     */
    public final List<JsonDocumentMutationResult> upsertManyJsonDocuments(List<JsonDocument> documents) {
        return mapJsonDocumentMutationResultList(upsertMany(mapJsonDocumentList(documents)));
    }

    /**
     * Upsert of up to 20 documents, expressed as key/value documents.
     *
     * @param documents
     *      list of documents
     * @return
     *      list of ids
     */
    public final CompletableFuture<List<JsonDocumentMutationResult>> upsertManyJsonDocumentsASync(List<JsonDocument> documents) {
        return  CompletableFuture.supplyAsync(() -> upsertManyJsonDocuments(documents));
    }

    /**
     * Insert a list of JsonDocument.
     *
     * @param documents
     *      document list
     * @return
     *      list of statuses when complete.
     */
    public final List<JsonDocumentMutationResult> upsertMany(JsonDocument... documents) {
        return upsertManyJsonDocuments(Arrays.asList(documents));
    }

    /**
     * Insert Asynchronously a list of documents.
     *
     * @param documents
     *      document list
     * @return
     *      list of statuses when complete.
     */
    public final CompletableFuture<List<JsonDocumentMutationResult>> upsertManyAsync(JsonDocument... documents) {
        return upsertManyJsonDocumentsASync(Arrays.asList(documents));
    }

    /**
     * Upsert any items in the collection.
     *
     * @param documents
     *      current collection list
     * @param <DOC>
     *     represent the pojo, payload of document
     * @return
     *      list of statuses
     */
    public final <DOC> List<DocumentMutationResult<DOC>> upsertMany(List<Document<DOC>> documents) {
        return insertMany(documents, true);
    }

    /**
     * Upsert of up to 20 documents, expressed as key/value documents.
     *
     * @param documents
     *      list of documents
     * @param <DOC>
     *     represent the pojo, payload of document
     * @return
     *      list of ids
     */
    public final <DOC> CompletableFuture<List<DocumentMutationResult<DOC>>> upsertManyASync(List<Document<DOC>> documents) {
        return  CompletableFuture.supplyAsync(() -> upsertMany(documents));
    }

    /**
     * Insert a list of documents.
     *
     * @param documents
     *      document list
     * @param <DOC>
     *     represent the pojo, payload of document
     * @return
     *      list of statuses when complete.
     */
    @SafeVarargs
    public final <DOC> List<DocumentMutationResult<DOC>> upsertMany(Document<DOC>... documents) {
        return upsertMany(Arrays.asList(documents));
    }

    /**
     * Insert Asynchronously a list of documents.
     *
     * @param documents
     *      document list
     * @param <DOC>
     *     represent the pojo, payload of document
     * @return
     *      list of statuses when complete.
     */
    @SafeVarargs
    public final  <DOC> CompletableFuture<List<DocumentMutationResult<DOC>>> upsertManyManyAsync(Document<DOC>... documents) {
        return upsertManyASync(Arrays.asList(documents));
    }

    /**
     * Insert a list of documents with the following constraints:
     * - the list should be smaller than 20, or we get errors
     * - the option 'ordered' is set to false in order to speed up the process
     * - the option 'replace' is set to false in order to  we do not replace documents
     *
     * @param documents
     *      list of documents
     * @param replaceIfExists
     *      if set to true existing documents will be replaced.
     * @param <DOC>
     *     represent the pojo, payload of document
     * @return
     *      list of ids
     */
    @SuppressWarnings("unchecked")
    private <DOC> List<DocumentMutationResult<DOC>> insertMany(List<Document<DOC>> documents, boolean replaceIfExists) {
        if (documents != null && !documents.isEmpty()) {
            log.debug("insert many (size={},ordered={}) into {}/{}", green(String.valueOf(documents.size())),
                    green(String.valueOf(insertManyOrdered)), green(namespaceClient.getNamespace()), green(collection));

            // Creating Status for output
            Map<String, DocumentMutationResult<DOC>> results = initResultMap(documents);

            // Insert documents synchronously
            ApiResponse apiResponse = execute("insertMany",
                    Map.of("documents", documents, "options",
                    Map.of("ordered", insertManyOrdered)));

            validate(apiResponse);
            if (apiResponse.getStatus() != null) {
                Optional.ofNullable(
                        apiResponse.getStatus().get("insertedIds")
                ).ifPresent(ids -> ((List<String>) ids)
                     .forEach(id -> results.computeIfPresent(id, (k, v) -> {
                        v.setStatus(DocumentMutationStatus.CREATED);
                        return v;
                     }))
                );
            }

            // Identify documents already existing
            if (apiResponse.getErrors()!=null) {
                Pattern pattern = Pattern.compile("'(.*?)'");
                apiResponse.getErrors()
                        .stream()
                        .filter(error -> "DOCUMENT_ALREADY_EXISTS".equals(error.getErrorCode()))
                        .map(ApiError::getMessage)
                        .map(pattern::matcher)
                        .filter(Matcher::find)
                        .map(matcher -> matcher.group(1))
                        .forEach(id -> results.computeIfPresent(id, (k, v) -> {
                            v.setStatus(DocumentMutationStatus.ALREADY_EXISTS);
                            return v;
                        }));
            }

            // Update ALREADY_EXISTS items
            if (replaceIfExists) {
                ExecutorService executor = Executors.newFixedThreadPool(10);
                results.values()
                        .stream()
                        .filter(r -> DocumentMutationStatus.ALREADY_EXISTS.equals(r.getStatus()))
                        .forEach(r -> executor.submit(() -> {
                            JsonResultUpdate u = findOneAndReplace(UpdateQuery.builder()
                                    .filter(new Filter().where("_id").isEqualsTo(r.getDocument().getId()))
                                    .replaceBy(r.getDocument())
                                    .build());
                            if (u.getUpdateStatus().getModifiedCount() == 0) {
                                r.setStatus(DocumentMutationStatus.UNCHANGED);
                            } else {
                                r.setStatus(DocumentMutationStatus.UPDATED);
                            }
                        }));
                executor.shutdown();
                try {
                    boolean ok = executor.awaitTermination(20L, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    throw new IllegalStateException("insert many is not finished", e);
                }
            }
            return new ArrayList<>(results.values());
        }
        return new ArrayList<>();
    }

    // ---------------------------------
    // ---  Insert Many  Chunked   ----
    // ---------------------------------

    /**
     * Low level insertion of multiple records
     *
     * @param json
     *      Json String
     * @param chunkSize
     *      size of the block
     * @param concurrency
     *      number of blocks in parallel
     * @return
     *      list of ids
     */
    public final List<JsonDocumentMutationResult> insertManyChunked(String json, int chunkSize, int concurrency) {
        return insertManyJsonDocumentsChunked(mapJsonStringToJsonDocumentList(json), chunkSize, concurrency);
    }

    /**
     * Low level insertion of multiple records
     *
     * @param json
     *      Json String
     * @param chunkSize
     *      size of the block
     * @param concurrency
     *      number of blocks in parallel
     * @return
     *      list of ids
     */
    public final CompletableFuture<List<JsonDocumentMutationResult>> insertManyChunkedASync(String json, int chunkSize, int concurrency) {
        return CompletableFuture.supplyAsync(() -> insertManyChunked(json, chunkSize, concurrency));
    }

    /**
     * Low level insertion of multiple records
     *
     * @param documents
     *      list of documents
     * @param chunkSize
     *      size of the block
     * @param concurrency
     *      number of blocks in parallel
     * @return
     *      list of ids
     */
    public final List<JsonDocumentMutationResult> insertManyJsonDocumentsChunked(List<JsonDocument> documents, int chunkSize, int concurrency) {
        return mapJsonDocumentMutationResultList(insertManyChunked(mapJsonDocumentList(documents), chunkSize, concurrency));
    }

    /**
     * Low level insertion of multiple records
     *
     * @param documents
     *      list of documents
     * @param chunkSize
     *      size of the block
     * @param concurrency
     *      number of blocks in parallel
     * @return
     *      list of ids
     */
    public final CompletableFuture<List<JsonDocumentMutationResult>> insertManyJsonDocumentsChunkedASync(List<JsonDocument> documents, int chunkSize, int concurrency) {
        return CompletableFuture.supplyAsync(() -> insertManyJsonDocumentsChunked(documents, chunkSize, concurrency));
    }

    /**
     * Low level insertion of multiple records
     *
     * @param documents
     *      list of documents
     * @param chunkSize
     *      size of the block
     * @param concurrency
     *      number of blocks in parallel
     * @param <DOC>
     *     represent the pojo, payload of document
     * @return
     *      list of ids
     */
    public final <DOC> List<DocumentMutationResult<DOC>> insertManyChunked(List<Document<DOC>> documents, int chunkSize, int concurrency) {
        return insertManyChunked(documents, chunkSize, concurrency,false);
    }

    /**
     * Low level insertion of multiple records asynchronously
     *
     * @param documents
     *      list of documents
     * @param chunkSize
     *      size of the block
     * @param concurrency
     *      number of blocks in parallel
     * @param <DOC>
     *     represent the pojo, payload of document
     * @return
     *      list of ids
     */
    public final <DOC> CompletableFuture<List<DocumentMutationResult<DOC>>> insertManyChunkedASync(List<Document<DOC>> documents, int chunkSize, int concurrency) {
        return CompletableFuture.supplyAsync(() -> insertManyChunked(documents, chunkSize, concurrency));
    }

    // ---------------------------------
    // ---  Upsert Many  Chunked   ----
    // ---------------------------------

    /**
     * Low level insertion of multiple records
     *
     * @param json
     *      Json String
     * @param chunkSize
     *      size of the block
     * @param concurrency
     *      number of blocks in parallel
     * @return
     *      list of ids
     */
    public final List<JsonDocumentMutationResult> upsertManyChunked(String json, int chunkSize, int concurrency) {
        return upsertManyJsonDocumentsChunked(mapJsonStringToJsonDocumentList(json), chunkSize, concurrency);
    }

    /**
     * Low level insertion of multiple records
     *
     * @param json
     *      Json String
     * @param chunkSize
     *      size of the block
     * @param concurrency
     *      number of blocks in parallel
     * @return
     *      list of ids
     */
    public final CompletableFuture<List<JsonDocumentMutationResult>> upsertManyChunkedASync(String json, int chunkSize, int concurrency) {
        return CompletableFuture.supplyAsync(() -> upsertManyChunked(json, chunkSize, concurrency));
    }

    /**
     * Low level insertion of multiple records
     *
     * @param documents
     *      list of documents
     * @param chunkSize
     *      size of the block
     * @param concurrency
     *      number of blocks in parallel
     * @return
     *      list of ids
     */
    public final List<JsonDocumentMutationResult> upsertManyJsonDocumentsChunked(List<JsonDocument> documents, int chunkSize, int concurrency) {
        return mapJsonDocumentMutationResultList(insertManyChunked(mapJsonDocumentList(documents), chunkSize, concurrency));
    }

    /**
     * Low level insertion of multiple records
     *
     * @param documents
     *      list of documents
     * @param chunkSize
     *      size of the block
     * @param concurrency
     *      number of blocks in parallel
     * @return
     *      list of ids
     */
    public final CompletableFuture<List<JsonDocumentMutationResult>> upsertManyJsonDocumentsChunkedASync(List<JsonDocument> documents, int chunkSize, int concurrency) {
        return CompletableFuture.supplyAsync(() -> upsertManyJsonDocumentsChunked(documents, chunkSize, concurrency));
    }

    /**
     * Low level insertion of multiple records
     *
     * @param documents
     *      list of documents
     * @param chunkSize
     *      size of the block
     * @param concurrency
     *      concurrency
     * @param <DOC>
     *     represent the pojo, payload of document
     * @return
     *      list of ids
     */
    public final <DOC> List<DocumentMutationResult<DOC>> upsertManyChunked(List<Document<DOC>> documents, int chunkSize, int concurrency) {
        return insertManyChunked(documents, chunkSize, concurrency,true);
    }

    /**
     * Low level insertion of multiple records
     *
     * @param documents
     *      list of documents
     * @param chunkSize
     *      size of the block
     * @param concurrency
     *      concurrency
     * @param <DOC>
     *     represent the pojo, payload of document
     * @return
     *      list of ids
     */
    public final <DOC> CompletableFuture<List<DocumentMutationResult<DOC>>> upsertManyChunkedASync(List<Document<DOC>> documents, int chunkSize, int concurrency) {
        return CompletableFuture.supplyAsync(() -> upsertManyChunked(documents, chunkSize, concurrency));
    }

    /**
     * Low level insertion of multiple records
     *
     * @param documents
     *      list of documents
     * @param chunkSize
     *      size of the block
     * @param concurrency
     *      how many threads to use
     * @param <DOC>
     *     represent the pojo, payload of document
     * @return
     *      list of ids
     */
    private <DOC> List<DocumentMutationResult<DOC>> insertManyChunked(List<Document<DOC>> documents, int chunkSize, int concurrency, boolean replaceIfExists) {
        // Validations
        if (chunkSize < 1 || chunkSize > 20) {
            throw new IllegalArgumentException("ChunkSize must be between 1 and 20");
        }
        //if (concurrency < 1 || concurrency > 50) {
        //    throw new IllegalArgumentException("Concurrency must be between 1 and 50");
       // }

        // Creating Status for output
        Map<String, DocumentMutationResult<DOC>> results = initResultMap(documents);

        ExecutorService executor = Executors.newFixedThreadPool(concurrency);
        List<Future<List<DocumentMutationResult<DOC>>>> futures = new ArrayList<>();
        for (int i = 0; i < documents.size(); i += chunkSize) {
            int start = i;
            int end = Math.min(i + chunkSize, documents.size());
            Callable<List<DocumentMutationResult<DOC>>> task = () -> {
                log.debug("insert block (size={}) {}/{}", end-start, green(namespaceClient.getNamespace()), green(collection));
                return insertMany(documents.subList(start, end), replaceIfExists);
            };
            futures.add(executor.submit(task));
        }

        // Wait for all futures to completes
        for (Future<List<DocumentMutationResult<DOC>>> future : futures) {
            try {
                future.get().forEach(r -> results.computeIfPresent(r.getDocument().getId(), (k, v) -> {
                    v.setStatus(r.getStatus());
                    return v;
                }));
            } catch (Exception e) {
                throw new RuntimeException("Error when process a block", e);
            }
        }

        return new ArrayList<>(results.values());
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
        log.debug("Counting {}/{}", green(namespaceClient.getNamespace()), green(collection));
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
        Filter findById = new Filter().where("_id").isEqualsTo(id);
        return findOne(SelectQuery.builder()
                    .select("_id")
                    .filter(findById).build())
                .isPresent();
    }

    /**
     * Find one document matching the query.
     *
     * @param query
     *      query documents and vector
     * @return
     *      result if exists
     */
    public Optional<JsonDocumentResult> findOne(SelectQuery query) {
        log.debug("Query in {}/{}", green(namespaceClient.getNamespace()), green(collection));
        return Optional.ofNullable(execute("findOne", query).getData().getDocument());
    }

    /**
     * Find one document matching the query.
     *
     * @param rawJsonQuery
     *      execute a direct json Query
     * @return
     *      result if exists
     */
    public Optional<JsonDocumentResult> findOne(String rawJsonQuery) {
        log.debug("Query in {}/{}", green(namespaceClient.getNamespace()), green(collection));
        return Optional.ofNullable(execute("findOne", rawJsonQuery).getData().getDocument());
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
    public <DOC> Optional<DocumentResult<DOC>> findOne(String query, Class<DOC> clazz) {
        return findOne(query).map(r -> new DocumentResult<>(r, clazz));
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
    public <DOC> Optional<DocumentResult<DOC>> findOne(String query, DocumentResultMapper<DOC> mapper) {
        return findOne(query).map(mapper::map);
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
    public <DOC> Optional<DocumentResult<DOC>> findOne(SelectQuery query, Class<DOC> clazz) {
        return findOne(query).map(r -> new DocumentResult<>(r, clazz));
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
    public <DOC> Optional<DocumentResult<DOC>> findOne(SelectQuery query, DocumentResultMapper<DOC> mapper) {
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
    public Optional<JsonDocumentResult> findById(String id) {
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
    public <DOC > Optional<DocumentResult<DOC>> findById(@NonNull String id, Class<DOC> clazz) {
        return findById(id).map(r -> new DocumentResult<>(r, clazz));
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
    public <DOC> Optional<DocumentResult<DOC>> findById(@NonNull String id, DocumentResultMapper<DOC> mapper) {
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
    public Optional<JsonDocumentResult> findOneByVector(float[] vector) {
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
    public <DOC> Optional<DocumentResult<DOC>> findOneByVector(float[] vector, Class<DOC> clazz) {
        return findOneByVector(vector).map(r -> new DocumentResult<>(r, clazz));
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
    public <DOC > Optional<DocumentResult<DOC>> findOneByVector(float[] vector, DocumentResultMapper<DOC> mapper) {
        return findOneByVector(vector).map(mapper::map);
    }

    // --------------------------
    // ---       Find        ----
    // --------------------------

    /**
     * Search records with a filter
     *
     * @param query
     *      filter
     * @return
     *      all items
     */
    public Stream<JsonDocumentResult> find(SelectQuery query) {
        List<JsonDocumentResult> documents = new ArrayList<>();
        String pageState;
        AtomicInteger pageCount = new AtomicInteger(0);
        do {
            log.debug("Fetching page " + pageCount.incrementAndGet());
            Page<JsonDocumentResult> pageX = findPage(query);
            pageState = pageX.getPageState().orElse(null);

            // We do not need all items of this page as limit is exceed
            if (query.getLimit().isPresent() &&
                    documents.size() + pageX.getResults().size() > query.getLimit().get()) {
                documents.addAll(pageX.getResults().subList(0, query.getLimit().get() - documents.size()));
                break;
            }
            documents.addAll(pageX.getResults());
            // Reusing query for next page
            query.setPageState(pageState);
        } while(pageState != null);
        return documents.stream();
    }

    /**
     * Find documents matching the query.
     *
     * @param query
     *      current query
     * @return
     *      page of results
     */
    public Page<JsonDocumentResult> findPage(SelectQuery query) {
        log.debug("Query in {}/{}", green(namespaceClient.getNamespace()), green(collection));
        ApiData apiData = execute("find", query).getData();
        int pageSize = (query != null && query.getLimit().isPresent()) ? query.getLimit().get() : SelectQuery.PAGING_SIZE_MAX;
        return new Page<>(pageSize, apiData.getNextPageState(), apiData.getDocuments());
    }

    /**
     * Find one document matching the query.
     *
     * @param query
     *      execute a direct json Query
     * @return
     *      result if exists
     */
    public Page<JsonDocumentResult> findPage(String query) {
        log.debug("Query in {}/{}", green(namespaceClient.getNamespace()), green(collection));
        ApiData apiData = execute("find", query).getData();
        return new Page<>(20, apiData.getNextPageState(), apiData.getDocuments());
    }

    // ------------------------------
    // ---  Similarity Search    ----
    // ------------------------------

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
    public Stream<JsonDocumentResult> findVector(float[] vector, Integer limit) {
        return findVector(vector, null, limit);
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
    public Stream<JsonDocumentResult> findVector(float[] vector, Filter filter, Integer limit) {
        return find(SelectQuery.builder()
                .filter(filter)
                .orderByAnn(vector)
                .withLimit(limit)
                .includeSimilarity()
                .build());
    }


    /**
     * find Page.
     *
     * @param query
     *      return query Page
     * @return
     *      page of results
     */
    public Page<JsonDocumentResult> findVectorPage(SelectQuery query) {
        return findPage(query);
    }

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
    public Page<JsonDocumentResult> findVectorPage(float[] vector, Filter filter, Integer limit, String pagingState) {
        return findVectorPage(SelectQuery.builder()
                .filter(filter)
                .orderByAnn(vector)
                .withLimit(limit)
                .withPagingState(pagingState)
                .includeSimilarity()
                .build());
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
    public <DOC> Page<DocumentResult<DOC>> findVectorPage(float[] vector, Filter filter, Integer limit, String pagingState, Class<DOC> clazz) {
        return mapPageJsonResultAsPageResult(findVectorPage(vector, filter, limit, pagingState), clazz);
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
    public <DOC> Page<DocumentResult<DOC>> findVectorPage(float[] vector, Filter filter, Integer limit, String pagingState, DocumentResultMapper<DOC> mapper) {
        return mapPageJsonResultAsPageResult(findVectorPage(vector, filter, limit, pagingState), mapper);
    }

    /**
     * Search records with a filter
     *
     * @param query
     *      filter
     * @param clazz
     *      class for target pojo
     * @return
     *      all items
     * @param <DOC>
     *       class to be marshalled
     */
    public  <DOC> Stream<DocumentResult<DOC>> find(SelectQuery query, Class<DOC> clazz) {
        return find(query).map(r -> new DocumentResult<>(r, clazz));
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
    public  <DOC> Stream<DocumentResult<DOC>> find(SelectQuery pageQuery, DocumentResultMapper<DOC> mapper) {
        return find(pageQuery).map(mapper::map);
    }

    /**
     * Get all items in a collection.
     *
     * @return
     *      all items
     */
    public Stream<JsonDocumentResult> findAll() {
        return find(SelectQuery.builder().build());
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
    public <DOC> Stream<DocumentResult<DOC>> findAll(Class<DOC> clazz) {
        return findAll().map(r -> new DocumentResult<>(r, clazz));
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
    public <DOC> Stream<DocumentResult<DOC>> findAll(DocumentResultMapper<DOC> mapper) {
        return findAll().map(mapper::map);
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
     * @param <DOC>
     *     class to be marshalled
     */
    public <DOC> Page<DocumentResult<DOC>> findPage(SelectQuery query, Class<DOC> clazz) {
        return mapPageJsonResultAsPageResult(findPage(query), clazz);
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
     * @param <DOC>
     *     class to be marshalled
     */
    public <DOC> Page<DocumentResult<DOC>> findPage(String query, Class<DOC> clazz) {
        return mapPageJsonResultAsPageResult(findPage(query), clazz);
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
    public <DOC > Page<DocumentResult<DOC>> findPage(SelectQuery query, DocumentResultMapper<DOC> mapper) {
        return mapPageJsonResultAsPageResult(findPage(query), mapper);
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
    public <DOC> Page<DocumentResult<DOC>> findPage(String query, DocumentResultMapper<DOC> mapper) {
        return mapPageJsonResultAsPageResult(findPage(query), mapper);
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
    public <DOC> Page<DocumentResult<DOC>> mapPageJsonResultAsPageResult(Page<JsonDocumentResult> pageJson, Class<DOC> clazz) {
        return new Page<>(
                pageJson.getPageSize(),
                pageJson.getPageState().orElse(null),
                pageJson.getResults()
                        .stream()
                        .map(r -> new DocumentResult<>(r, clazz))
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
    public <DOC> Page<DocumentResult<DOC>> mapPageJsonResultAsPageResult(Page<JsonDocumentResult> pageJson, DocumentResultMapper<DOC> mapper) {
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
     *      number of deleted records and status
     */
    public DeleteResult deleteOne(DeleteQuery deleteQuery) {
        log.debug("Delete in {}/{}", green(namespaceClient.getNamespace()), green(collection));
        return new DeleteResult(execute("deleteOne", deleteQuery));
    }

    /**
     * Delete single record from its id.
     *
     * @param id
     *      id
     * @return
     *      number of deleted records
     */
    public DeleteResult deleteById(String id) {
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
    public DeleteResult deleteByVector(float[] vector) {
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
    public DeleteResult deleteMany(DeleteQuery deleteQuery) {
        AtomicInteger totalCount = new AtomicInteger(0);
        DeleteResult res;
        do {
            res = new DeleteResult(execute("deleteMany", deleteQuery));
            totalCount.addAndGet(res.getDeletedCount());
        } while(res.isMoreData());
        return new DeleteResult(totalCount.get(), false);
    }

    /**
     * Perform a distributed deleted.
     * @param deleteQuery
     *      deleting query
     * @param concurrency
     *      concrrency number
     * @return
     *      the delete result
     */
    public DeleteResult deleteManyChunked(DeleteQuery deleteQuery, int concurrency) {
        if (concurrency < 1 || concurrency > 50) {
            throw new IllegalArgumentException("Concurrency must be between 1 and 50");
        }
        AtomicInteger totalCount =
                new AtomicInteger(0);
        ExecutorService executor =
                Executors.newFixedThreadPool(concurrency);
        ExecutorCompletionService<DeleteResult> completionService =
                new ExecutorCompletionService<>(executor);
        try {
            for (int i = 0; i < concurrency; i++) {
                completionService.submit(() -> deleteManyPaged(deleteQuery));
            }

            int activeTasks = concurrency;

            while (activeTasks > 0) {
                Future<DeleteResult> completedFuture = completionService.take();
                DeleteResult result = completedFuture.get();
                totalCount.addAndGet(result.getDeletedCount());
                if (result.isMoreData()) {
                    completionService.submit(() -> deleteManyPaged(deleteQuery));
                } else {
                    activeTasks--;
                }
            }

        } catch (Exception e) {
            throw new IllegalStateException("Cannot delete chunked in a distributed mode", e);
        } finally {
            executor.shutdown();
            try {
                executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        return new DeleteResult(totalCount.get(), false);
    }

    /**
     * Delete multiple records from a request.
     *
     * @param deleteQuery
     *      delete query
     * @return
     *      number of deleted records
     */
    public DeleteResult deleteManyPaged(DeleteQuery deleteQuery) {
        log.debug("Delete in {}/{}", green(namespaceClient.getNamespace()), green(collection));
        return new DeleteResult(execute("deleteMany", deleteQuery));
    }

    /**
     * Clear the collection.
     *
     * @return
     *      number of items deleted
     */
    public DeleteResult deleteAll() {
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
     *     result of the update
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
     *      query to use
     * @return
     *      returned object by the Api
     */
    private JsonResultUpdate updateQuery(String operation, UpdateQuery query) {
        log.debug("{} in {}/{}", operation, green(namespaceClient.getNamespace()), green(collection));
        ApiResponse response = execute(operation, query);
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
        log.debug("updateOne in {}/{}", green(namespaceClient.getNamespace()), green(collection));
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
        log.debug("updateMany in {}/{}", green(namespaceClient.getNamespace()), green(collection));
        return updateQuery("updateMany", query).getUpdateStatus();
    }

    // --------------------------
    // ---  Utilities        ----
    // --------------------------

    /**
     * Initialization of the collection of document, filling uid
     * @param documents
     *      document list
     * @return
     *      map of documents
     * @param <DOC>
     *     represent the pojo, payload of document
     */
    private static <DOC> Map<String, DocumentMutationResult<DOC>> initResultMap(List<Document<DOC>> documents) {
        Map<String, DocumentMutationResult<DOC>> results = new LinkedHashMap<>(documents.size());
        documents.forEach(d -> {
            if (d.getId() == null) {
                // Enforce the UUID at client side to retrieve it in an easier way
                d.setId(UUID.randomUUID().toString());
            }
            results.put(d.getId(), new DocumentMutationResult<>(d));
        });
        return results;
    }

    /**
     * Mapper for Json document list input.
     *
     * @param documents
     *      json document list
     * @return
     *      Document of Map
     */
    private List<Document<Map<String, Object>>> mapJsonDocumentList(List<JsonDocument> documents) {
        return documents.stream()
                .map(d -> (Document<Map<String, Object>>) d)
                .collect(Collectors.toList());
    }

    /**
     * Mapper for output Json Document.
     *
     * @param list
     *      document list
     * @return
     *      json document mutation
     */
    private List<JsonDocumentMutationResult> mapJsonDocumentMutationResultList(List<DocumentMutationResult<Map<String, Object>>> list) {
        return list.stream()
                .map(DocumentMutationResult::asJsonDocumentMutationResult)
                .collect(Collectors.toList());
    }

    /**
     * Marshalling a list of document from JSON.
     *
     * @param json
     *      current Json String
     * @return
     *      list of documents
     */
    @SuppressWarnings("unchecked")
    private List<JsonDocument> mapJsonStringToJsonDocumentList(String json) {
        List<Map<String, Object>> kvList = JsonUtils.unmarshallBeanForDataApi(json, List.class);
        // Marshall as a list of Document
        return kvList.stream().map(JsonDocument::new).collect(Collectors.toList());
    }

    /**
     * Syntax sugar.
     *
     * @param operation
     *      operation to run
     * @param payload
     *      payload returned
     */
    private ApiResponse execute(String operation, Object payload) {
       return executeOperation(namespaceClient.getDataApiClient().getStargateHttpClient(), collectionResource, operation, payload);
    }

}
