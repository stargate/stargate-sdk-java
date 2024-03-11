package io.stargate.sdk.data.internal;

import io.stargate.sdk.data.client.DataApiCollection;
import io.stargate.sdk.data.client.DataApiNamespace;
import io.stargate.sdk.data.client.exception.CollectionNotFoundException;
import io.stargate.sdk.data.client.exception.TooManyDocumentsException;
import io.stargate.sdk.data.client.model.BulkWriteOptions;
import io.stargate.sdk.data.client.model.BulkWriteResult;
import io.stargate.sdk.data.client.model.CreateCollectionOptions;
import io.stargate.sdk.data.client.model.DeleteResult;
import io.stargate.sdk.data.client.model.DistinctIterable;
import io.stargate.sdk.data.client.model.Document;
import io.stargate.sdk.data.client.model.Filter;
import io.stargate.sdk.data.client.model.FindIterable;
import io.stargate.sdk.data.client.model.InsertManyOptions;
import io.stargate.sdk.data.client.model.InsertManyResult;
import io.stargate.sdk.data.client.model.ReplaceOptions;
import io.stargate.sdk.data.client.model.UpdateOptions;
import io.stargate.sdk.data.client.model.UpdateResult;
import io.stargate.sdk.data.client.model.find.FindOneAndDeleteOptions;
import io.stargate.sdk.data.client.model.find.FindOneAndReplaceOptions;
import io.stargate.sdk.data.client.model.find.FindOneAndUpdateOptions;
import io.stargate.sdk.data.client.model.find.FindOneCommand;
import io.stargate.sdk.data.client.model.find.FindOneOptions;
import io.stargate.sdk.data.client.model.insert.InsertOneResult;
import io.stargate.sdk.data.internal.model.ApiResponse;
import io.stargate.sdk.data.internal.model.CollectionInformation;
import io.stargate.sdk.http.LoadBalancedHttpClient;
import io.stargate.sdk.http.ServiceHttp;
import io.stargate.sdk.utils.Assert;
import io.stargate.sdk.utils.JsonUtils;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static io.stargate.sdk.data.internal.DataApiUtils.runCommand;
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
        Optional<CollectionInformation> optCol = namespace
                .listCollections()
                .filter(col -> col.getName().equals(collectionName))
                .findFirst();
        if (optCol.isEmpty()) {
            throw new CollectionNotFoundException(collectionName);
        }
        if (optCol.get().getOptions() == null) {
            return new CreateCollectionOptions();
        }
        return optCol.get().getOptions();
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
        notNull(document, "document");
        ApiResponse apiResponse = execute("insertOne", Map.of("document", document));
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

    public Optional<Document> findOne(Filter filter, FindOneOptions options) {
        ApiResponse apiResponse = execute("findOne", new FindOneCommand(filter, options));
        return Optional.ofNullable(apiResponse.getData().getDocument());
    }

    // ----------------------------
    // ---   Count Document    ----
    // ----------------------------

    @Override
    public long countDocuments() throws TooManyDocumentsException {
        return 0;
    }

    @Override
    public long countDocuments(Filter filter) throws TooManyDocumentsException {
        return 0;
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
            ApiResponse apiResponse = execute("deleteMany", filter);
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

    /**
     * Syntax sugar.
     *
     * @param operation
     *      operation to run
     * @param payload
     *      payload returned
     */
    private ApiResponse execute(String operation, Object payload) {
        return DataApiUtils.runCommand(getHttpClient(), collectionResource, operation, payload);
    }

}
