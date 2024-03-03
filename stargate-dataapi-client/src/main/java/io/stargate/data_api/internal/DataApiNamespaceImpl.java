package io.stargate.data_api.internal;

import io.stargate.data_api.client.DataApiCollection;
import io.stargate.data_api.client.DataApiNamespace;
import io.stargate.data_api.client.model.CreateCollectionOptions;
import io.stargate.data_api.client.model.Document;
import io.stargate.data_api.internal.model.CreateCollectionRequest;
import lombok.NonNull;

import java.util.stream.Stream;

class DataApiNamespaceImpl implements DataApiNamespace {

    public DataApiNamespaceImpl(DataApiClientImpl apiClient, String namespace) {
    }

    @Override
    public String getName() {
        return null;
    }

    @Override
    public DataApiCollection<Document> getCollection(@NonNull String collectionName) {
        return null;
    }

    @Override
    public <TDocument> DataApiCollection<TDocument> getCollection(@NonNull String collectionName, @NonNull Class<TDocument> tDocumentClass) {
        return null;
    }

    @Override
    public Document runCommand(@NonNull Object command) {
        return null;
    }

    @Override
    public <TResult> TResult runCommand(@NonNull Object command, @NonNull Class<TResult> tResultClass) {
        return null;
    }

    @Override
    public void drop() {

    }

    @Override
    public Stream<String> listCollectionNames() {
        return null;
    }

    @Override
    public Stream<CreateCollectionRequest> listCollections() {
        return null;
    }

    @Override
    public void dropCollection(@NonNull String collectionName) {
        
    }

    @Override
    public DataApiCollection<Document> createCollection(@NonNull String collectionName, CreateCollectionOptions createCollectionOptions) {
        return null;
    }
}
