package io.stargate.sdk.data.test.integration;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.stargate.sdk.data.client.DataApiClient;
import io.stargate.sdk.data.client.DataApiCollection;
import io.stargate.sdk.data.client.DataApiNamespace;
import io.stargate.sdk.data.client.exception.TooManyDocumentsException;
import io.stargate.sdk.data.client.model.Document;
import io.stargate.sdk.data.client.model.SimilarityMetric;
import io.stargate.sdk.data.client.model.collections.CreateCollectionOptions;
import io.stargate.sdk.data.client.model.find.FindOneOptions;
import io.stargate.sdk.data.client.model.insert.InsertOneResult;
import io.stargate.sdk.data.internal.model.ApiResponse;
import io.stargate.sdk.data.test.TestConstants;
import io.stargate.sdk.utils.JsonUtils;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import java.util.Map;
import java.util.Optional;

import static io.stargate.sdk.data.client.DataApiLimits.MAX_DOCUMENTS_COUNT;
import static io.stargate.sdk.data.client.model.Filters.eq;
import static io.stargate.sdk.data.client.model.Filters.gt;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public abstract class AbstractCollectionITTest  implements TestConstants {

    /** Tested Store. */
    static DataApiClient apiClient;

    /** Tested Namespace. */
    static DataApiNamespace namespace;

    /** Tested collection1. */
    protected static DataApiCollection<Document> collectionSimple;

    /** Tested collection2. */
    protected static DataApiCollection<Product> collectionVector;

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Product {
        @JsonProperty("_id")
        private Object objectId;
        @JsonProperty("product_name")
        private String name;
        @JsonProperty("product_price")
        private Double price;
    }

    protected DataApiCollection<Document> getCollectionSimple() {
        if (collectionSimple == null) {
            collectionSimple = getDataApiNamespace().createCollection(COLLECTION_SIMPLE);
        }
        return collectionSimple;
    }

    protected DataApiCollection<Product> getCollectionVector() {
        if (collectionVector == null) {
            collectionVector = getDataApiNamespace().createCollection(COLLECTION_VECTOR,
                    CreateCollectionOptions
                            .builder()
                            .withVectorDimension(14)
                            .withVectorSimilarityMetric(SimilarityMetric.cosine)
                            .build(), Product.class);
        }
        return collectionVector;
    }

    protected synchronized DataApiNamespace getDataApiNamespace() {
        if (namespace == null) {
            AbstractCollectionITTest.namespace = initNamespace();
        }
        return namespace;
    }

    protected abstract DataApiNamespace initNamespace();

    @Test
    @Order(1)
    public void shouldPopulateGeneralInformation() {
        assertThat(getCollectionSimple().getOptions()).isNotNull();
        assertThat(getCollectionSimple().getName()).isNotNull();
        assertThat(getCollectionSimple().getDocumentClass()).isNotExactlyInstanceOf(Document.class);
        assertThat(getCollectionSimple().getNamespace()).isNotNull();
        assertThat(getCollectionVector().getOptions()).isNotNull();
        assertThat(getCollectionVector().getName()).isNotNull();
        assertThat(getCollectionVector().getDocumentClass()).isNotExactlyInstanceOf(Document.class);
        assertThat(getCollectionVector().getNamespace()).isNotNull();
    }

    @Test
    @Order(2)
    public void testInsertOne() {
        // Given
        InsertOneResult res1 = getCollectionSimple()
                .insertOne(new Document().append("hello", "world"));
        // Then
        assertThat(res1).isNotNull();
        assertThat(res1.getInsertedId()).isNotNull();

        Product product = new Product(null, "cool", 9.99);
        InsertOneResult res2 = collectionVector.insertOne(product);
        assertThat(res2).isNotNull();
        assertThat(res2.getInsertedId()).isNotNull();
    }

    @Test
    public void testFindOne() {
        collectionVector.deleteAll();
        collectionVector.insertOne(new Product(1, "cool", 9.99));

        // Find One with no options
        Optional<Document> doc = collectionVector.findOne(eq(1));
        doc.ifPresent(d -> System.out.println(JsonUtils.marshallForDataApi(d.map(Product.class))));

        // Find One with a filter and projection
        Optional<Document> doc2 = collectionVector.findOne(eq(1), new FindOneOptions()
                .projection(Map.of("product_name",1)));
        doc2.ifPresent(d -> System.out.println(JsonUtils.marshallForDataApi(d.map(Product.class))));

        // Find One with a projection only
        Optional<Document> doc3 = collectionVector.findOne(null, new FindOneOptions()
                .projection(Map.of("product_name",1)));
    }

    @Test
    public void testRunCommand() {
        collectionSimple.deleteAll();
        String insertOne = "{\"insertOne\":{\"document\":{\"_id\":1, \"product_name\":\"hello\"}}}";
        ApiResponse res = collectionSimple.runCommand(insertOne);
        assertThat(res).isNotNull();
        assertThat(res.getStatus().getList("insertedIds", Object.class).get(0)).isEqualTo(1);

        String findOne   = "{\"findOne\":{\"filter\":{\"_id\":1}}}";
        res = collectionSimple.runCommand(findOne);
        assertThat(res).isNotNull();
        assertThat(res.getData()).isNotNull();
        assertThat(res.getData().getDocument()).isNotNull();
        Document doc = res.getData().getDocument();
        assertThat(doc.getString("product_name")).isEqualTo("hello");

        Document doc2 = collectionSimple.runCommand(findOne, Document.class);
        assertThat(doc2).isNotNull();

        Product p1 = collectionSimple.runCommand(findOne, Product.class);
        assertThat(p1).isNotNull();
        assertThat(p1.getName()).isEqualTo("hello");
    }

    @Test
    public void testCountDocument() throws TooManyDocumentsException {

        assertThatThrownBy(() -> getCollectionSimple().countDocuments(-1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("UpperBound");
        assertThatThrownBy(() -> getCollectionSimple().countDocuments(2000))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("UpperBound");

        getCollectionSimple().deleteAll();
        // Insert 10 documents
        for(int i=0;i<10;i++) {
            getCollectionSimple().insertOne(new Document().id(i).append("indice", i));
        }
        assertThat(getCollectionSimple().countDocuments(1000)).isEqualTo(10);
        assertThatThrownBy(() -> getCollectionSimple().countDocuments(9))
                .isInstanceOf(TooManyDocumentsException.class)
                .hasMessageContaining("upper bound ");

        // Add a filter
        assertThat(getCollectionSimple()
                .countDocuments(gt("indice", 3), MAX_DOCUMENTS_COUNT))
                .isEqualTo(6);

        // Filter + limit
        for(int i=11;i<1005;i++) {
            getCollectionSimple().insertOne(new Document().id(i).append("indice", i));
        }

        // More than 1000 items
        assertThatThrownBy(() -> getCollectionSimple().countDocuments(MAX_DOCUMENTS_COUNT))
                .isInstanceOf(TooManyDocumentsException.class)
                .hasMessageContaining("server");
    }

}
