package io.stargate.sdk.data.test.integration;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.stargate.sdk.data.client.DataApiClient;
import io.stargate.sdk.data.client.DataApiCollection;
import io.stargate.sdk.data.client.DataApiNamespace;
import io.stargate.sdk.data.client.exception.TooManyDocumentsToCountException;
import io.stargate.sdk.data.client.model.DataApiCommand;
import io.stargate.sdk.data.client.model.DataApiResponse;
import io.stargate.sdk.data.client.model.Document;
import io.stargate.sdk.data.client.model.SimilarityMetric;
import io.stargate.sdk.data.client.model.SortOrder;
import io.stargate.sdk.data.client.model.collections.CreateCollectionOptions;
import io.stargate.sdk.data.client.model.find.FindOneOptions;
import io.stargate.sdk.data.client.model.find.FindOptions;
import io.stargate.sdk.data.client.model.insert.InsertManyOptions;
import io.stargate.sdk.data.client.model.insert.InsertOneResult;
import io.stargate.sdk.data.client.observer.LoggerCommandObserver;
import io.stargate.sdk.data.test.TestConstants;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

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

    /**
     * Generating sample document to insert.
     *
     * @param size
     *      size of the list
     * @return
     *      number of documents
     */
    protected List<Document> generateDocList(int size) {
        return  IntStream
                .range(0, size)
                .mapToObj(idx ->Document.create(idx).append("indice", idx))
                .collect(Collectors.toList());
    }

    protected DataApiCollection<Document> getCollectionSimple() {
        if (collectionSimple == null) {
            collectionSimple = getDataApiNamespace().createCollection(COLLECTION_SIMPLE);
            collectionSimple.registerListener("logger", new LoggerCommandObserver(DataApiCollection.class));
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
            collectionVector.registerListener("logger", new LoggerCommandObserver(DataApiCollection.class));
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
        InsertOneResult res2 = getCollectionVector().insertOne(product);
        assertThat(res2).isNotNull();
        assertThat(res2.getInsertedId()).isNotNull();
    }

    @Test
    public void testFindOne() {
        getCollectionVector().deleteAll();
        getCollectionVector().insertOne(new Product(1, "cool", 9.99));

        // Find One with no options
        Optional<Product> doc = getCollectionVector().findOne(eq(1));

        // Find One with a filter and projection
        Optional<Product> doc2 = getCollectionVector().findOne(eq(1), new FindOneOptions()
                .projection(Map.of("product_name",1)));

        // Find One with a projection only
        Optional<Product> doc3 = getCollectionVector().findOne(null, new FindOneOptions()
                .projection(Map.of("product_name",1)));
    }

    @Test
    public void testRunCommand() {
        getCollectionSimple().deleteAll();

        DataApiResponse res = getCollectionSimple().runCommand(
                new DataApiCommand<>("insertOne",
                Map.of("document", new Document().id(1).append("product_name", "hello"))));
        assertThat(res).isNotNull();
        assertThat(res.getStatus().getList("insertedIds", Object.class).get(0)).isEqualTo(1);

        DataApiCommand<?> findOne = new DataApiCommand<>("findOne",Map.of("filter", new Document().id(1)));
        res = getCollectionSimple().runCommand(findOne);
        assertThat(res).isNotNull();
        assertThat(res.getData()).isNotNull();
        assertThat(res.getData().getDocument()).isNotNull();
        Document doc = res.getData().getDocument();
        assertThat(doc.getString("product_name")).isEqualTo("hello");

        Document doc2 = getCollectionSimple().runCommand(findOne, Document.class);
        assertThat(doc2).isNotNull();

        Product p1 = getCollectionSimple().runCommand(findOne, Product.class);
        assertThat(p1).isNotNull();
        assertThat(p1.getName()).isEqualTo("hello");
    }

    @Test
    public void testCountDocument() throws TooManyDocumentsToCountException {

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
                .isInstanceOf(TooManyDocumentsToCountException.class)
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
                .isInstanceOf(TooManyDocumentsToCountException.class)
                .hasMessageContaining("server");
    }

    @Test
    public void testFind() {
        // Populate the Collection
        getCollectionSimple().deleteAll();
        for(int i=0;i<25;i++) getCollectionSimple().insertOne(Document.create(i).append("indice", i));

        // Retrieve
        FindOptions options = new FindOptions().sortingBy("indice", SortOrder.ASCENDING).skip(11).limit(2);
        List<Document> documents = getCollectionSimple().find(options).all();
        assertThat(documents.size()).isEqualTo(2);
        assertThat(documents.get(0).getInteger("indice")).isEqualTo(11);
        assertThat(documents.get(1).getInteger("indice")).isEqualTo(12);
    }

    @Test
    public void testInsertManySinglePage() throws TooManyDocumentsToCountException {
        getCollectionSimple().deleteAll();
        List<Document> docList = generateDocList(10);
        getCollectionSimple().insertMany(docList);
        assertThat(getCollectionSimple().countDocuments(100)).isEqualTo(10);
    }

    @Test
    public void testInsertManyWithPaging() throws TooManyDocumentsToCountException {
        getCollectionSimple().deleteAll();
        List<Document> docList = generateDocList(25);
        getCollectionSimple().insertMany(docList);
        assertThat(getCollectionSimple().countDocuments(100)).isEqualTo(25);
    }

    @Test
    public void testInsertManyWithPagingDistributed() throws TooManyDocumentsToCountException {
        getCollectionSimple().deleteAll();
        List<Document> docList = generateDocList(55);
        getCollectionSimple().insertMany(docList, InsertManyOptions.builder().concurrency(5).build());
        assertThat(getCollectionSimple().countDocuments(100)).isEqualTo(55);
    }



}
