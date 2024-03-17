package io.stargate.sdk.data.internal;

import io.stargate.sdk.core.domain.Page;
import io.stargate.sdk.data.client.DataApiCollection;
import io.stargate.sdk.data.client.DataApiLimits;
import io.stargate.sdk.data.client.DataApiNamespace;
import io.stargate.sdk.data.client.exception.DataApiException;
import io.stargate.sdk.data.client.exception.DataApiFaultyResponseException;
import io.stargate.sdk.data.client.exception.DataApiResponseException;
import io.stargate.sdk.data.client.exception.TooManyDocumentsToCountException;
import io.stargate.sdk.data.client.model.DataApiCommand;
import io.stargate.sdk.data.client.model.delete.CommandDeleteMany;
import io.stargate.sdk.data.client.model.delete.CommandDeleteOne;
import io.stargate.sdk.data.client.model.delete.DeleteOneOptions;
import io.stargate.sdk.data.client.model.find.FindOneAndReplaceCommand;
import io.stargate.sdk.data.client.model.find.FindOneAndReplaceResult;
import io.stargate.sdk.data.client.model.iterable.FindIterable;
import io.stargate.sdk.data.client.model.DataApiResponse;
import io.stargate.sdk.data.client.model.iterable.DistinctIterable;
import io.stargate.sdk.data.client.model.Document;
import io.stargate.sdk.data.client.model.Filter;
import io.stargate.sdk.data.client.model.collections.CollectionDefinition;
import io.stargate.sdk.data.client.model.collections.CollectionOptions;
import io.stargate.sdk.data.client.model.delete.DeleteResult;
import io.stargate.sdk.data.client.model.find.CommandFind;
import io.stargate.sdk.data.client.model.find.CommandFindOne;
import io.stargate.sdk.data.client.model.find.FindOneAndDeleteOptions;
import io.stargate.sdk.data.client.model.find.FindOneAndReplaceOptions;
import io.stargate.sdk.data.client.model.find.FindOneAndUpdateOptions;
import io.stargate.sdk.data.client.model.find.FindOneOptions;
import io.stargate.sdk.data.client.model.find.FindOptions;
import io.stargate.sdk.data.client.model.insert.CommandInsertMany;
import io.stargate.sdk.data.client.model.insert.CommandInsertOne;
import io.stargate.sdk.data.client.model.insert.InsertManyOptions;
import io.stargate.sdk.data.client.model.insert.InsertManyResult;
import io.stargate.sdk.data.client.model.insert.InsertOneResult;
import io.stargate.sdk.data.client.model.misc.BulkWriteOptions;
import io.stargate.sdk.data.client.model.misc.BulkWriteResult;
import io.stargate.sdk.data.client.model.misc.CommandCountDocuments;
import io.stargate.sdk.data.client.model.misc.CountDocumentsResult;
import io.stargate.sdk.data.client.model.update.ReplaceOneOptions;
import io.stargate.sdk.data.client.model.update.UpdateOptions;
import io.stargate.sdk.data.client.model.update.UpdateResult;
import io.stargate.sdk.http.LoadBalancedHttpClient;
import io.stargate.sdk.http.ServiceHttp;
import io.stargate.sdk.utils.Assert;
import io.stargate.sdk.utils.JsonUtils;
import lombok.Getter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

import static io.stargate.sdk.data.client.model.Filters.and;
import static io.stargate.sdk.data.client.model.Filters.eq;
import static io.stargate.sdk.utils.AnsiUtils.cyan;
import static io.stargate.sdk.utils.AnsiUtils.green;
import static io.stargate.sdk.utils.AnsiUtils.magenta;
import static io.stargate.sdk.utils.AnsiUtils.yellow;
import static io.stargate.sdk.utils.Assert.hasLength;
import static io.stargate.sdk.utils.Assert.notNull;

/**
 * Class representing a Data Api Collection.
 *
 * @param <DOC>
 *     working document
 */
@Slf4j
public class DataApiCollectionImpl<DOC> extends AbstractApiClient implements DataApiCollection<DOC> {

    /** Collection identifier. */
    @Getter
    private final String collectionName;

    /** Keep ref to the generic. */
    protected final Class<DOC> documentClass;

    /** keep reference to namespace client. */
    private final DataApiNamespace namespace;

    /** Resource collection. */
    public final Function<ServiceHttp, String> collectionResource;

    /**
     * Full constructor.
     *
     * @param namespaceClient
     *      client namespace http
     * @param collectionName
     *      collection identifier
     */
    protected DataApiCollectionImpl(DataApiNamespace namespaceClient,String collectionName, Class<DOC> clazz) {
        hasLength(collectionName, "collectionName");
        notNull(namespaceClient, "namespace client");
        this.collectionName     = collectionName;
        this.namespace          = namespaceClient;
        this.documentClass      = clazz;
        this.collectionResource = (node) -> namespaceClient.lookup().apply(node) + "/" + collectionName;
    }

    // ----------------------------
    // --- Global Informations ----
    // ----------------------------

    /** {@inheritDoc} */
    @Override
    public DataApiNamespace getNamespace() {
        return namespace;
    }

    /** {@inheritDoc} */
    @Override
    public CollectionDefinition getDefinition() {
        return namespace
                .listCollections()
                .filter(col -> col.getName().equals(collectionName))
                .findFirst()
                .orElseThrow(() -> new DataApiException("[COLLECTION_NOT_EXIST] - Collection does not exist, " +
                        "collection name: '" + collectionName + "'", "COLLECTION_NOT_EXIST", null));
    }

    /** {@inheritDoc} */
    @Override
    public CollectionOptions getOptions() {
        return Optional
                .ofNullable(getDefinition()
                .getOptions())
                .orElse(new CollectionOptions());
    }

    /** {@inheritDoc} */
    @Override
    public Class<DOC> getDocumentClass() {
        return documentClass;
    }

    /** {@inheritDoc} */
    @Override
    public String getName() {
        return collectionName;
    }

    // --------------------------
    // ---   Insert*         ----
    // --------------------------

    /** {@inheritDoc} */
    @Override
    public final InsertOneResult insertOne(DOC document) {
        return new InsertOneResult(
                runCommand(new CommandInsertOne<>(document))
                        .getStatusKeyAsList("insertedIds", Object.class)
                        .get(0));
    }

    /** {@inheritDoc} */
    @Override
    public InsertManyResult insertMany(List<? extends DOC> documents) {
        return insertMany(documents, InsertManyOptions.builder().build());
    }

    /** {@inheritDoc} */
    @Override
    public InsertManyResult insertMany(List<? extends DOC> documents, InsertManyOptions options) {
        if (options.getConcurrency() > 1 && options.isOrdered()) {
            throw new IllegalArgumentException("Cannot run ordered insert_many concurrently.");
        }
        if (options.getChunkSize() > DataApiLimits.MAX_DOCUMENTS_IN_INSERT) {
            throw new IllegalArgumentException("Cannot insert more than " + DataApiLimits.MAX_DOCUMENTS_IN_INSERT + " at a time.");
        }

        // TODO handle the auto-inserted-ids here (chunk-wise better)

        long start = System.currentTimeMillis();
        ExecutorService executor = Executors.newFixedThreadPool(options.getConcurrency());
        List<Future<InsertManyResult>> futures = new ArrayList<>();
        for (int i = 0; i < documents.size(); i += options.getChunkSize()) {
            futures.add(executor.submit(getInsertManyResultCallable(documents, options, i)));
        }
        executor.shutdown();

        // Grouping All Insert ids in the same list.
        InsertManyResult finalResult = new InsertManyResult();
        try {
            for (Future<InsertManyResult> future : futures) {
                finalResult.getInsertedIds().addAll(future.get().getInsertedIds());
            }

            if (executor.awaitTermination(options.getTimeout(), TimeUnit.MILLISECONDS)) {
                log.debug(magenta(".[total insertMany.responseTime]") + "=" + yellow("{}") + " millis.",
                        System.currentTimeMillis() - start);
            } else {
                throw new TimeoutException("Request did not complete withing ");
            }
        } catch (Exception e) {
            throw new RuntimeException("Cannot merge call results into a InsertManyResult", e);
        }
        return finalResult;
    }

    /**
     * Execute a 1 for 1 call to the Data API.
     *
     * @param documents
     *      list of documents to be inserted
     * @param options
     *      options for insert many (chunk size and insertion order).
     * @param start
     *      offset in global list
     * @return
     *      insert many result for a paged call
     */
    private Callable<InsertManyResult> getInsertManyResultCallable(List<? extends DOC> documents, InsertManyOptions options, int start) {
        int end = Math.min(start + options.getChunkSize(), documents.size());
        return () -> {
            log.debug("Insert block (" + cyan("size={}") + ") in collection {}", end - start, green(getCollectionName()));
            return new InsertManyResult(runCommand(new CommandInsertMany<DOC>(
                    documents.subList(start, end), options.isOrdered()))
                        .getStatusKeyAsList("insertedIds", Object.class));
        };
    }

    // --------------------------
    // ---   Find*           ----
    // --------------------------

    /** {@inheritDoc} */
    @Override
    public Optional<DOC> findOne(Filter filter, FindOneOptions options) {
        return Optional.ofNullable(
                runCommand(new CommandFindOne()
                        .withFilter(filter)
                        .withOptions(options))
                        .getData().getDocument()
                        .map(getDocumentClass()));
    }

    /** {@inheritDoc} */
    @Override
    public FindIterable<DOC> find(Filter filter, FindOptions options) {
        return new FindIterable<>(this, filter, options);
    }

    /** {@inheritDoc} */
    @Override
    public Page<DOC> findPage(Filter filter, FindOptions options) {
        DataApiResponse dataApiResponse = runCommand(new CommandFind()
                .withFilter(filter)
                .withOptions(options));
        return new Page<>(DataApiLimits.MAX_PAGE_SIZE,
                dataApiResponse.getData().getNextPageState(),
                dataApiResponse.getData().getDocuments()
                        .stream()
                        .map(d -> d.map(getDocumentClass()))
                        .collect(Collectors.toList()));
    }

    /** {@inheritDoc} */
    @Override
    public <FIELD> DistinctIterable<DOC, FIELD> distinct(String fieldName, Filter filter, Class<FIELD> resultClass) {
        return new DistinctIterable<>(this, fieldName, filter, resultClass);
    }

    // ----------------------------
    // ---   Count Document    ----
    // ----------------------------

    /** {@inheritDoc} */
    @Override
    public int countDocuments(int upperBound) throws TooManyDocumentsToCountException {
        return countDocuments(null, upperBound);
    }

    /** {@inheritDoc} */
    @Override
    public int countDocuments(Filter filter, int upperBound) throws TooManyDocumentsToCountException {
        if (upperBound<1 || upperBound> DataApiLimits.MAX_DOCUMENTS_COUNT) {
            throw new IllegalArgumentException("UpperBound limit should be in between 1 and " + DataApiLimits.MAX_DOCUMENTS_COUNT);
        }
        DataApiResponse response = (filter == null) ?
                runCommand(new CommandCountDocuments()) :
                runCommand(new CommandCountDocuments(filter));
        CountDocumentsResult res = response.getStatus().map(CountDocumentsResult.class);
        if (res.getMoreData() != null && res.getMoreData()) {
            throw new TooManyDocumentsToCountException();
        } else if (res.getCount() > upperBound) {
            throw new TooManyDocumentsToCountException(upperBound);
        }
        return res.getCount();
    }

    // ----------------------------
    // ---   Delete            ----
    // ----------------------------

    public static final String DELETED_COUNT = "deletedCount";

    public static final String MATCHED_COUNT = "matchedCount";
    public static final String MODIFIED_COUNT = "modifiedCount";
    public static final String MORE_DATA = "moreData";



    /** {@inheritDoc} */
    @Override
    public DeleteResult deleteOne(Filter filter, DeleteOneOptions deleteOneOptions) {
        CommandDeleteOne deleteOne = new CommandDeleteOne().withFilter(filter).withOptions(deleteOneOptions);
        DataApiResponse dataApiResponse = runCommand(deleteOne);
        int deletedCount = dataApiResponse.getStatus().getInteger(DELETED_COUNT);
        return new DeleteResult(deletedCount);
    }

    /** {@inheritDoc} */
    @Override
    public DeleteResult deleteMany(Filter filter) {
        Assert.notNull(filter, "filter");
        AtomicInteger totalCount = new AtomicInteger(0);
        DeleteResult res;
        boolean moreData = false;
        do {
            CommandDeleteMany deleteMany = new CommandDeleteMany().withFilter(filter);
            DataApiResponse dataApiResponse = runCommand(deleteMany);
            Document status = dataApiResponse.getStatus();
            if (status != null) {
                if (status.containsKey(DELETED_COUNT)) {
                    totalCount.addAndGet(status.getInteger(DELETED_COUNT));
                }
                if (status.containsKey(MORE_DATA)) {
                    moreData = status.getBoolean(MORE_DATA);
                }
            }
        } while(moreData);
        return new DeleteResult(totalCount.get());
    }

    /** {@inheritDoc} */
    @Override
    public DeleteResult deleteAll() {
        return deleteMany(new Filter());
    }

    /** {@inheritDoc} */
    @Override
    public void drop() {
        getNamespace().dropCollection(collectionName);
    }

    // ----------------------------
    // ---  Update             ----
    // ----------------------------

    /** {@inheritDoc} */
    @Override
    public Optional<DOC> findOneAndReplace(Filter filter, DOC replacement, FindOneAndReplaceOptions options) {
        return Optional.ofNullable(executeFindOneAndReplace(
                new FindOneAndReplaceCommand<DOC>()
                    .withFilter(filter)
                    .withReplacement(replacement)
                    .withOptions(options))
                .getDocument());
    }

    /** {@inheritDoc} */@Override
    public UpdateResult replaceOne(Filter filter, DOC replacement, ReplaceOneOptions replaceOneOptions) {

        // Build options for a replace
        FindOneAndReplaceOptions options = new FindOneAndReplaceOptions()
                .upsert(replaceOneOptions.getUpsert())
                .returnDocument(FindOneAndReplaceOptions.ReturnDocument.before);

        // Execute the `findOneAndReplace`
        FindOneAndReplaceResult<DOC> res = executeFindOneAndReplace(new FindOneAndReplaceCommand<DOC>()
                .withFilter(filter)
                .withReplacement(replacement)
                .withOptions(options));

        // Parse the result for a replace one
        UpdateResult result = new UpdateResult();
        result.setMatchedCount(res.getMatchedCount());
        result.setModifiedCount(res.getModifiedCount());
        if (res.getDocument() != null) {
            Document doc = JsonUtils.convertValueForDataApi(res.getDocument(), Document.class);
            if (doc.getId(Object.class) != null) {
                result.setUpsertedId(doc.getId(Object.class));
            }
        }
        return result;
    }

    /**
     * Mutualisation of the code for replaceOne() and findOneAndReplaceOne().
     *
     * @param cmd
     *      command
     * @return
     *      command result
     */
    private FindOneAndReplaceResult<DOC> executeFindOneAndReplace(FindOneAndReplaceCommand<DOC> cmd) {
        // Run Command
        DataApiResponse dataApiResponse = runCommand(cmd);
        // Parse Command Result
        FindOneAndReplaceResult<DOC> result = new FindOneAndReplaceResult<DOC>();
        if (dataApiResponse.getData() == null) {
            throw new DataApiFaultyResponseException(cmd, dataApiResponse ,"Faulty response from find_one_and_replace API command.");
        }
        if (dataApiResponse.getData().getDocument() != null) {
            result.setDocument(dataApiResponse
                    .getData()
                    .getDocument()
                    .map(getDocumentClass()));
        }
        Document status = dataApiResponse.getStatus();
        if (status != null) {
            if (status.containsKey(MATCHED_COUNT)) {
                result.setMatchedCount(status.getInteger(MATCHED_COUNT));
            }
            if (status.containsKey(MODIFIED_COUNT)) {
                result.setModifiedCount(status.getInteger(MODIFIED_COUNT));
            }
        }
        return result;
    }

    /** {@inheritDoc} */
    @Override
    public Optional<DOC> findOneAndDelete(Filter filter, FindOneAndDeleteOptions options) {
        return Optional.empty();
    }

    /** {@inheritDoc} */
    @Override
    public Optional<DOC> findOneAndUpdate(Filter filter, Object update, FindOneAndUpdateOptions options) {
        return Optional.empty();
    }

    /** {@inheritDoc} */
    @Override
    public UpdateResult updateOne(Filter filter, Object update, UpdateOptions updateOptions) {
        return null;
    }

    /** {@inheritDoc} */
    @Override
    public UpdateResult updateMany(Filter filter, Object update, UpdateOptions updateOptions) {
        return null;
    }



    // ----------------------------
    // ---   Bulk Write        ----
    // ----------------------------

    /** {@inheritDoc} */
    @Override
    public BulkWriteResult bulkWrite(List<DataApiCommand<?>> requests, BulkWriteOptions options) {
        return null;
    }

    // ------------------------------------------
    // ----           Lookup                 ----
    // ------------------------------------------

    /** {@inheritDoc} */
    @Override
    public Function<ServiceHttp, String> lookup() {
        return collectionResource;
    }

    /** {@inheritDoc} */
    @Override
    public LoadBalancedHttpClient getHttpClient() {
        return getNamespace().getHttpClient();
    }

}
