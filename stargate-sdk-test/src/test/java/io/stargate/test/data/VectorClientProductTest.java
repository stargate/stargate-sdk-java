package io.stargate.test.data;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.stargate.sdk.core.domain.Page;
import io.stargate.sdk.data.DataApiClient;
import io.stargate.sdk.data.CollectionClient;
import io.stargate.sdk.data.CollectionRepository;
import io.stargate.sdk.data.DocumentMutationResult;
import io.stargate.sdk.data.domain.CollectionDefinition;
import io.stargate.sdk.data.domain.query.Filter;
import io.stargate.sdk.data.domain.JsonDocument;
import io.stargate.sdk.data.domain.JsonDocumentResult;
import io.stargate.sdk.data.domain.NamespaceDefinition;
import io.stargate.sdk.data.domain.query.SelectQuery;
import io.stargate.sdk.data.domain.odm.Document;
import io.stargate.sdk.data.domain.odm.DocumentResult;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static io.stargate.sdk.data.domain.SimilarityMetric.cosine;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class VectorClientProductTest {

    @Data @NoArgsConstructor @AllArgsConstructor
    static class Product {

        @JsonProperty("product_name")
        private String name;

        @JsonProperty("product_price")
        private Double price;
    }

    static final String NAMESPACE = "demo";
    static final String COLLECTION = "sample_collection";
    static final String COLLECTION_VECTOR = "pet_supply_vectors";

    static DataApiClient jsonApi;

    static CollectionClient myCollection;

    static CollectionRepository<Product> vectorStore;

    @BeforeAll
    public static void setup() {
        // If default (localhost:8181) no parameter needed
        jsonApi = new DataApiClient();
        jsonApi.dropNamespace(NAMESPACE);
    }

    @Test
    @Order(1)
    @DisplayName("01. Create a namespace and a collection")
    public void shouldCreateCollection() {

        jsonApi.createNamespace(NAMESPACE);

        jsonApi.createNamespace(NamespaceDefinition.builder()
                .name(NAMESPACE + "_bis")
                .simpleStrategy(1)
                //.networkTopologyStrategy(Collections.singletonMap("dc1", 1))
                .build());
        Assertions.assertTrue(jsonApi.findAllNamespaces()
                .collect(Collectors.toList())
                .contains(NAMESPACE));

        // Create a collection
        jsonApi.namespace(NAMESPACE).createCollection(COLLECTION);
        jsonApi.namespace(NAMESPACE).createCollection(CollectionDefinition
                .builder()
                .name(COLLECTION_VECTOR)
                .vector(14, cosine)
                .build());
        List<String> collections = jsonApi.namespace(NAMESPACE)
                .findCollections()
                .map(CollectionDefinition::getName)
                .collect(Collectors.toList());
        Assertions.assertTrue(collections.contains(COLLECTION));
        Assertions.assertTrue(collections.contains(COLLECTION_VECTOR));

        // Vector Store Experience
        myCollection = jsonApi.namespace(NAMESPACE).collection(COLLECTION_VECTOR);
        vectorStore  = jsonApi.namespace(NAMESPACE).collectionRepository(COLLECTION_VECTOR, Product.class);

        float[] sampleVector = new float[] {1f, 1f, 1f, 1f, 1f, 0f, 0f, 0f, 0f, 0f, 0f, 0f, 0f, 0f};
        DocumentMutationResult<Product> resVector = vectorStore.insert(new Document<Product>().id("1").data(
                new Product("HealthyFresh - Beef raw dog food", 12.99)).vector(
                sampleVector));

        Assertions.assertTrue(vectorStore.findById(resVector.getDocument().getId()).isPresent());
        Assertions.assertTrue(vectorStore.findByVector(sampleVector).isPresent());
    }

    @Test
    @Order(2)
    @DisplayName("02. Insert Records")
    public void shouldInsertVectors() {
        // (Insert Many)
        myCollection.insertMany(List.of(
                // Add keys
                new JsonDocument("pf1844")
                        .put("product_name", "HealthyFresh - Beef raw dog food")
                        .put("product_price", 12.99)
                        .vector(new float[] {1f, 0f, 1f, 1f, 1f, 1f, 0f, 0f, 0f, 0f, 0f, 0f, 0f, 0f}),
                new JsonDocument("pf1843")
                        .vector(new float[] {1f, 1f, 1f, 1f, 1f, 0f, 0f, 0f, 0f, 0f, 0f, 0f, 0f, 0f})
                        .data(Map.of("product_name", "HealthyFresh - Chicken raw dog food")),
                new JsonDocument()
                        .id("pt0021")
                        .data("{ \"product_name\": \"Dog Tennis Ball Toy\" }")
                        .vector(new float[] {0f, 0f, 0f, 1f, 0f, 0f, 0f, 0f, 0f, 1f, 1f, 1f, 0f, 0f})));
        myCollection.insertMany(List.of(
                // Builder and bean
                new Document<Product>()
                        .id("pt0041")
                        .data(new Product("Dog Ring Chew Toy", 9.99))
                        .vector(new float[] {0f, 0f, 0f, 1f, 0f, 0f, 0f, 1f, 1f, 1f, 0f, 0f, 0f, 0f}),
                new Document<Product>()
                        .id("pf7043")
                        .data(new Product("Pepper Sausage Bacon dog Treats", 9.99))
                        .vector(new float[] {0f, 0f, 0f, 1f, 0f, 0f, 1f, 0f, 0f, 0f, 0f, 0f, 1f, 1f}),
                new Document<Product>()
                        .id("pf7044")
                        .data(new Product("Pepper Sausage Beef dog Treats", 9.99))
                        .vector(new float[] {0f, 0f, 0f, 1f, 0f, 1f, 1f, 0f, 0f, 0f, 0f, 0f, 1f, 0f})));
    }

    @Test
    @Order(3)
    @DisplayName("03. Count Records")
    public void shouldCountRecords() {

         // Default Count
         myCollection.countDocuments();

         // Count with a Filter
         myCollection.countDocuments(new Filter()
             .where("product_price")
              .isEqualsTo(9.99));
    }

    @Test
    @Order(4)
    @DisplayName("04. Similarity Search")
    public void shouldSimilaritySearch() {
        CollectionClient myCollection = jsonApi
                .namespace(NAMESPACE)
                .collection(COLLECTION_VECTOR);

        Page<JsonDocumentResult> page = myCollection.findPage(SelectQuery.builder()
                .includeSimilarity()
                .orderByAnn(new float[]{1f, 1f, 1f, 1f, 1f, 0f, 0f, 0f, 0f, 0f, 0f, 0f, 0f, 0f})
                .withLimit(2)
                .build());

        Page<DocumentResult<Product>> pageProduct = myCollection.findPage(SelectQuery.builder()
                .includeSimilarity()
                .orderByAnn(new float[]{1f, 1f, 1f, 1f, 1f, 0f, 0f, 0f, 0f, 0f, 0f, 0f, 0f, 0f})
                .withLimit(2)
                .build(), Product.class);

        System.out.println("Result Size=" + page.getPageSize());
        System.out.println("Result Size=" + pageProduct.getPageSize());
        for(JsonDocumentResult result : page.getResults()) {
            System.out.println(result.getId() + ") similarity=" + result.getSimilarity() + ", vector=" +
                    Arrays.toString(result.getVector()));
        }
    }

    @Test
    @Order(5)
    @DisplayName("05. Meta Data Filtering")
    public void shouldMetaDataFiltering() {
            Page<JsonDocumentResult> page =  myCollection.findPage(SelectQuery.builder()
                    .includeSimilarity()
                    .where("product_price").isEqualsTo(9.99)
                    .orderByAnn(new float[]{1f, 1f, 1f, 1f, 1f, 0f, 0f, 0f, 0f, 0f, 0f, 0f, 0f, 0f})
                    .withLimit(2)
                    .build());

            System.out.println("Result Size=" + page.getPageSize());
            for(JsonDocumentResult result : page.getResults()) {
                System.out.println(result.getId() + ") similarity=" + result.getSimilarity() + ", vector=" +
                        Arrays.toString(result.getVector()));
            }
    }

}
