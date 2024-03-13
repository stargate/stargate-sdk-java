package io.stargate.sdk.data.internal;

import io.stargate.sdk.data.client.DataApiCollection;
import io.stargate.sdk.data.client.DataApiLimits;
import io.stargate.sdk.data.client.DataApiNamespace;
import io.stargate.sdk.data.client.exception.DataApiException;
import io.stargate.sdk.data.client.exception.TooManyDocumentsException;
import io.stargate.sdk.data.client.model.misc.BulkWriteOptions;
import io.stargate.sdk.data.client.model.misc.BulkWriteResult;
import io.stargate.sdk.data.client.model.Command;
import io.stargate.sdk.data.client.model.find.CommandFindOne;
import io.stargate.sdk.data.client.model.insert.CommandInsertOne;
import io.stargate.sdk.data.client.model.collections.CreateCollectionOptions;
import io.stargate.sdk.data.client.model.delete.DeleteResult;
import io.stargate.sdk.data.client.model.DistinctIterable;
import io.stargate.sdk.data.client.model.Document;
import io.stargate.sdk.data.client.model.Filter;
import io.stargate.sdk.data.client.model.FindIterable;
import io.stargate.sdk.data.client.model.insert.InsertManyOptions;
import io.stargate.sdk.data.client.model.insert.InsertManyResult;
import io.stargate.sdk.data.client.model.misc.CommandCountDocuments;
import io.stargate.sdk.data.client.model.misc.CountDocumentsResult;
import io.stargate.sdk.data.client.model.update.ReplaceOptions;
import io.stargate.sdk.data.client.model.update.UpdateOptions;
import io.stargate.sdk.data.client.model.update.UpdateResult;
import io.stargate.sdk.data.client.model.find.FindOneAndDeleteOptions;
import io.stargate.sdk.data.client.model.find.FindOneAndReplaceOptions;
import io.stargate.sdk.data.client.model.find.FindOneAndUpdateOptions;
import io.stargate.sdk.data.client.model.find.FindOneOptions;
import io.stargate.sdk.data.client.model.insert.InsertOneResult;
import io.stargate.sdk.data.internal.model.ApiResponse;
import io.stargate.sdk.data.internal.model.CollectionDefinition;
import io.stargate.sdk.http.LoadBalancedHttpClient;
import io.stargate.sdk.http.ServiceHttp;
import io.stargate.sdk.utils.Assert;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static io.stargate.sdk.utils.Assert.hasLength;
import static io.stargate.sdk.utils.Assert.notNull;

/**
 * Class representing a Data Api Collection.
 *
 * @param <DOC>
 *     working document
 */
@Slf4j
public class DataApiCollectionImpl<DOC> implements DataApiCollection<DOC> {

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
        this.collectionName  = collectionName;
        this.namespace       = namespaceClient;
        this.documentClass   = clazz;
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
    public CreateCollectionOptions getOptions() {
        Optional<CollectionDefinition> optCol = namespace
                .listCollections()
                .filter(col -> col.getName().equals(collectionName))
                .findFirst();
        if (optCol.isEmpty()) {
            throw new DataApiException("[COLLECTION_NOT_EXIST] - Collection does not exist, " +
                    "collection name: '" + collectionName + "'", "COLLECTION_NOT_EXIST", null);
        }
        CollectionDefinition collectionDefinition = optCol.get();
        return (collectionDefinition.getOptions() == null) ?
                new CreateCollectionOptions() :
                collectionDefinition.getOptions();
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
    // ---   Insert One      ----
    // --------------------------

    /** {@inheritDoc} */
    @Override
    public final InsertOneResult insertOne(DOC document) {
        ApiResponse apiResponse = runCommand(new CommandInsertOne<>(document));
        if (apiResponse.getErrors() != null && !apiResponse.getErrors().isEmpty()) {
            apiResponse.getErrors().get(0).throwDataApiException();
        }
        return new InsertOneResult(apiResponse
                .getStatusKeyAsList("insertedIds", String.class)
                .get(0));
    }

    // --------------------------
    // ---   Find One        ----
    // --------------------------

    /** {@inheritDoc} */
    @Override
    public Optional<Document> findOne(Filter filter, FindOneOptions options) {
        ApiResponse apiResponse = runCommand(new CommandFindOne().withFilter(filter).withOptions(options));
        return Optional.ofNullable(apiResponse.getData().getDocument());
    }

    @Override
    public FindIterable<DOC> find() {
        return null;
    }

    @Override
    public <T> FindIterable<T> find(Class<T> resultClass) {
        return null;
    }

    @Override
    public FindIterable<DOC> find(Filter filter) {
        return null;
    }

    @Override
    public <T> FindIterable<T> find(Filter filter, Class<T> resultClass) {
        return null;
    }

    // ----------------------------
    // ---   Count Document    ----
    // ----------------------------

    /** {@inheritDoc} */
    @Override
    public long countDocuments(int upperBound) throws TooManyDocumentsException {
        return countDocuments(null, upperBound);
    }

    /** {@inheritDoc} */
    @Override
    public long countDocuments(Filter filter, int upperBound) throws TooManyDocumentsException {
        if (upperBound<1 || upperBound> DataApiLimits.MAX_DOCUMENTS_COUNT) {
            throw new IllegalArgumentException("UpperBound limit should be in between 1 and " + DataApiLimits.MAX_DOCUMENTS_COUNT);
        }
        ApiResponse response = (filter == null) ?
                runCommand(new CommandCountDocuments()) :
                runCommand(new CommandCountDocuments(filter));
        CountDocumentsResult res = response.getStatus().map(CountDocumentsResult.class);
        if (res.getMoreData() != null && res.getMoreData()) {
            throw new TooManyDocumentsException();
        } else if (res.getCount() > upperBound) {
            throw new TooManyDocumentsException(upperBound);
        }
        return res.getCount();
    }

    @Override
    public <FIELD> DistinctIterable<FIELD> distinct(String fieldName, Class<FIELD> resultClass) {
        return null;
    }

    @Override
    public <FIELD> DistinctIterable<FIELD> distinct(String fieldName, Filter filter, Class<FIELD> resultClass) {
        return null;
    }

    @Override
    public BulkWriteResult bulkWrite(List<String> requests) {
        return null;
    }

    @Override
    public BulkWriteResult bulkWrite(List<String> requests, BulkWriteOptions options) {
        return null;
    }

    @Override
    public InsertManyResult insertMany(List<? extends DOC> documents) {
        return null;
    }

    @Override
    public InsertManyResult insertMany(List<? extends DOC> documents, InsertManyOptions options) {
        return null;
    }

    @Override
    public DeleteResult deleteOne(Filter filter) {
        return null;
    }

    /** {@inheritDoc} */
    @Override
    public DeleteResult deleteMany(Filter filter) {
        Assert.notNull(filter, "filter");
        AtomicInteger totalCount = new AtomicInteger(0);
        DeleteResult res;
        boolean moreData = false;
        do {
            ApiResponse apiResponse = runCommand(new Command<>("deleteMany", filter));
            DataApiUtils.validate(apiResponse);
            Document status = apiResponse.getStatus();
            if (status != null) {
                if (status.containsKey("deletedCount")) {
                    totalCount.addAndGet(status.getInteger("deletedCount"));
                }
                if (status.containsKey("moreData")) {
                    moreData = status.getBoolean("moreData");
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

    @Override
    public UpdateResult replaceOne(Filter filter, DOC replacement) {
        return null;
    }

    @Override
    public UpdateResult replaceOne(Filter filter, DOC replacement, ReplaceOptions replaceOptions) {
        return null;
    }

    @Override
    public UpdateResult updateOne(Filter filter, Object update) {
        return null;
    }

    @Override
    public UpdateResult updateOne(Filter filter, Object update, UpdateOptions updateOptions) {
        return null;
    }

    @Override
    public UpdateResult updateMany(Filter filter, Object update) {
        return null;
    }

    @Override
    public UpdateResult updateMany(Filter filter, Object update, UpdateOptions updateOptions) {
        return null;
    }

    @Override
    public Optional<DOC> findOneAndDelete(Filter filter) {
        return Optional.empty();
    }

    @Override
    public Optional<DOC> findOneAndDelete(Filter filter, FindOneAndDeleteOptions options) {
        return Optional.empty();
    }

    @Override
    public Optional<DOC> findOneAndReplace(Filter filter, DOC replacement) {
        return Optional.empty();
    }

    @Override
    public Optional<DOC> findOneAndReplace(Filter filter, DOC replacement, FindOneAndReplaceOptions options) {
        return Optional.empty();
    }

    @Override
    public Optional<DOC> findOneAndUpdate(Filter filter, Object update) {
        return Optional.empty();
    }

    @Override
    public Optional<DOC> findOneAndUpdate(Filter filter, Object update, FindOneAndUpdateOptions options) {
        return Optional.empty();
    }

    /** {@inheritDoc} */
    @Override
    public void drop() {
        getNamespace().dropCollection(collectionName);
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
