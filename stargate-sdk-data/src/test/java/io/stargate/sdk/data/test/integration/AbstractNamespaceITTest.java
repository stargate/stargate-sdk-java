package io.stargate.sdk.data.test.integration;

import io.stargate.sdk.data.client.DataApiCollection;
import io.stargate.sdk.data.client.DataApiNamespace;
import io.stargate.sdk.data.client.exception.DataApiException;
import io.stargate.sdk.data.client.model.DataApiCommand;
import io.stargate.sdk.data.client.model.collections.CommandCreateCollection;
import io.stargate.sdk.data.client.model.collections.CreateCollectionOptions;
import io.stargate.sdk.data.client.model.Document;
import io.stargate.sdk.data.client.model.SimilarityMetric;
import io.stargate.sdk.data.client.model.DataApiResponse;
import io.stargate.sdk.data.test.TestConstants;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import java.util.Map;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Super Class to run Tests against Data API.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public abstract class AbstractNamespaceITTest implements TestConstants {

    /**
     * Reference to working DataApiNamespace
     */
    public static DataApiNamespace namespace;

    /**
     * Initialization of the working Namespace.
     *
     * @return
     *      current Namespace
     */
    public DataApiNamespace getDataApiNamespace() {
        if (namespace == null) {
            AbstractNamespaceITTest.namespace = initNamespace();
        }
        return namespace;
    }

    /**
     * Initialization of the DataApiNamespace.
     *
     * @return
     *      the instance of Data ApiNamespace
     */
    protected abstract DataApiNamespace initNamespace();

    @Test
    @Order(1)
    public void shouldCreateCollectionSimple() {
        // When
        getDataApiNamespace().createCollection(COLLECTION_SIMPLE);
        assertThat(getDataApiNamespace().existCollection(COLLECTION_SIMPLE)).isTrue();
        // When
        DataApiCollection<Document> collection_simple = getDataApiNamespace().getCollection(COLLECTION_SIMPLE);
        assertThat(collection_simple).isNotNull();
        assertThat(collection_simple.getName()).isEqualTo(COLLECTION_SIMPLE);

        DataApiCollection<Document> c1 = getDataApiNamespace().createCollection(COLLECTION_SIMPLE, Document.class);
        assertThat(c1).isNotNull();
        assertThat(c1.getName()).isEqualTo(COLLECTION_SIMPLE);
    }

    @Test
    @Order(2)
    public void shouldCreateCollectionsVector() {
        DataApiCollection<Document> collectionVector = getDataApiNamespace().createCollection(COLLECTION_VECTOR,
                CreateCollectionOptions.builder()
                        .withVectorDimension(14)
                        .withVectorSimilarityMetric(SimilarityMetric.cosine)
                        .build());
        assertThat(collectionVector).isNotNull();
        assertThat(collectionVector.getName()).isEqualTo(COLLECTION_VECTOR);

        CreateCollectionOptions options = collectionVector.getOptions();
        assertThat(options.getVector()).isNotNull();
        assertThat(options.getVector().getDimension()).isEqualTo(14);
    }

    @Test
    @Order(3)
    public void shouldCreateCollectionsAllows() {
        DataApiCollection<Document> collectionAllow = getDataApiNamespace().createCollection(COLLECTION_ALLOW,
                CreateCollectionOptions.builder()
                        .withIndexingAllow("a", "b", "c")
                        .build());
        assertThat(collectionAllow).isNotNull();
        CreateCollectionOptions options = collectionAllow.getOptions();
        assertThat(options.getIndexing()).isNotNull();
        assertThat(options.getIndexing().getAllow()).isNotNull();
    }

    @Test
    @Order(4)
    public void shouldCreateCollectionsDeny() {
        DataApiCollection<Document> collectionDeny = getDataApiNamespace().createCollection(COLLECTION_DENY,
                CreateCollectionOptions.builder()
                        .withIndexingDeny("a", "b", "c")
                        .build());
        assertThat(collectionDeny).isNotNull();
        CreateCollectionOptions options = collectionDeny.getOptions();
        assertThat(options.getIndexing()).isNotNull();
        assertThat(options.getIndexing().getDeny()).isNotNull();
    }

    @Test
    @Order(5)
    public void shouldListCollections() {
        shouldCreateCollectionSimple();
        assertThat(getDataApiNamespace().listCollectionNames().collect(Collectors.toList())).isNotNull();
    }

    @Test
    @Order(6)
    public void shouldDropCollectionAllow() {
        // Given
        shouldCreateCollectionsAllows();
        assertThat(getDataApiNamespace().existCollection(COLLECTION_ALLOW)).isTrue();
        // When
        getDataApiNamespace().dropCollection(COLLECTION_ALLOW);
        // Then
        assertThat(getDataApiNamespace().existCollection(COLLECTION_ALLOW)).isFalse();
    }

    @Test
    @Order(6)
    public void shouldDropCollectionsDeny() {
        // Given
        DataApiCollection<Document> collectionDeny = getDataApiNamespace().createCollection(COLLECTION_DENY,
                CreateCollectionOptions.builder()
                        .withIndexingDeny("a", "b", "c")
                        .build());
        assertThat(getDataApiNamespace().existCollection(COLLECTION_DENY)).isTrue();
        // When
        collectionDeny.drop();
        // Then
        assertThat(getDataApiNamespace().existCollection(COLLECTION_DENY)).isFalse();
    }

    @Test
    @Order(7)
    public void shouldRunCommand() {
        // Create From String
        DataApiResponse res = getDataApiNamespace().runCommand(
                new CommandCreateCollection("collection_simple"));
        assertThat(res).isNotNull();
        assertThat(res.getStatus().getInteger("ok")).isEqualTo(1);

        // Create From Specialized command class
        DataApiResponse res2 = getDataApiNamespace().runCommand(
                new CommandCreateCollection("collection_simple"));
        assertThat(res2.getStatusKeyAsInt("ok")).isEqualTo(1);

        // Create From Generic command class
        DataApiResponse res3 = getDataApiNamespace().runCommand(
                new DataApiCommand<>("createCollection", Map.of("name", "collection_simple")));
        assertThat(res3.getStatusKeyAsInt("ok")).isEqualTo(1);
    }

    @Test
    @Order(8)
    public void shouldRunCommandTyped() {
        // Given
        DataApiCommand<?> listCollectionNames = new DataApiCommand<>("findCollections", null);
        Document doc = getDataApiNamespace().runCommand(listCollectionNames, Document.class);
        assertThat(doc).isNotNull();
        assertThat(doc.getList("collections", String.class)).isNotNull();
    }

    @Test
    @Order(8)
    public void shouldErrorGetIfCollectionDoesNotExists() {
        // Given
        DataApiCollection<Document> collection = getDataApiNamespace().getCollection("invalid");
        assertThat(collection).isNotNull();
        assertThat(getDataApiNamespace().existCollection("invalid")).isFalse();
        assertThatThrownBy(collection::getOptions)
                .isInstanceOf(DataApiException.class)
                .hasMessageContaining("COLLECTION_NOT_EXIST");
    }

    @Test
    @Order(9)
    public void shouldErrorDropIfCollectionDoesNotExists() {
        assertThat(getDataApiNamespace().existCollection("invalid")).isFalse();
        DataApiCollection<Document> invalid = getDataApiNamespace().getCollection("invalid");
        assertThat(invalid).isNotNull();
        assertThatThrownBy(() -> invalid.insertOne(new Document().append("hello", "world")))
                .isInstanceOf(DataApiException.class)
                .hasMessageContaining("COLLECTION_NOT_EXIST");
    }

}
