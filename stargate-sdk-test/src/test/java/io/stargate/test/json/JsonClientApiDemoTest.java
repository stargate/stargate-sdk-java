package io.stargate.test.json;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.stargate.sdk.core.domain.Page;
import io.stargate.sdk.json.JsonDocumentsClient;
import io.stargate.sdk.json.StargateJsonApiClient;
import io.stargate.sdk.json.domain.CollectionDefinition;
import io.stargate.sdk.json.domain.JsonDocument;
import io.stargate.sdk.json.domain.JsonDocumentResult;
import io.stargate.sdk.json.domain.JsonFilter;
import io.stargate.sdk.json.domain.NamespaceDefinition;
import io.stargate.sdk.json.domain.Query;
import lombok.Builder;
import lombok.Data;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import java.util.List;
import java.util.Map;

import static io.stargate.sdk.json.domain.CollectionDefinition.SimilarityMetric.cosine;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class JsonClientApiDemoTest {

    @Data @Builder
    private static class Product {

        @JsonProperty("product_name")
        private String name;

        @JsonProperty("product_price")
        private Double price;
    }

    public static final String NAMESPACE = "demo";
    public static final String COLLECTION = "sample_collection";
    public static final String COLLECTION_VECTOR = "pet_supply_vectors";

    public static StargateJsonApiClient jsonApi;

    public static JsonDocumentsClient myCollection;

    @BeforeAll
    public static void setup() {
        // If default (localhost:8181) no parameter needed
        jsonApi = new StargateJsonApiClient();
        //jsonApi.dropNamespace(NAMESPACE);
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
        Assertions.assertTrue(jsonApi.findNamespaces().toList().contains(NAMESPACE));

        // Create a collection
        jsonApi.namespace(NAMESPACE).createCollection(COLLECTION);
        jsonApi.namespace(NAMESPACE).createCollection(CollectionDefinition
                .builder()
                .name(COLLECTION_VECTOR)
                .vector(14, cosine)
                //.vectorize(openai, "gpt3.5-turbo")
                .build());
        List<String> collections = jsonApi.namespace(NAMESPACE).findCollections().toList();
        Assertions.assertTrue(collections.contains(COLLECTION));
        Assertions.assertTrue(collections.contains(COLLECTION_VECTOR));

        // To be used later
        myCollection = jsonApi
                .namespace(NAMESPACE)
                .collection(COLLECTION_VECTOR);

    }

    @Test
    @Order(2)
    @DisplayName("02. Insert Records")
    public void shouldInsertVectors() {

        // (Insert Many)
        myCollection.insert(
                // Add keys
                new JsonDocument("pf1844")
                        .put("product_name", "HealthyFresh - Beef raw dog food")
                        .put("product_price", 12.99)
                        .vector(1d, 0d, 1d, 1d, 1d, 1d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d),
                // Constructor and Map
                new JsonDocument("pf1843")
                        .vector(1d, 1d, 1d, 1d, 1d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d)
                        .document(Map.of("product_name", "HealthyFresh - Chicken raw dog food")),
                // Builder and json String
                JsonDocument.builder()
                        .id("pt0021")
                        .jsonDocument("{ \"product_name\": \"Dog Tennis Ball Toy\" }")
                        .vector(0d, 0d, 0d, 1d, 0d, 0d, 0d, 0d, 0d, 1d, 1d, 1d, 0d, 0d)
                        .build(),
                // Builder and bean
                JsonDocument.builder()
                        .id("pt0041")
                        .document(new Product("Dog Ring Chew Toy", 9.99))
                        .vector(0d, 0d, 0d, 1d, 0d, 0d, 0d, 1d, 1d, 1d, 0d, 0d, 0d, 0d)
                        .build(),
                JsonDocument.builder()
                        .id("pf7043")
                        .document(new Product("PupperSausage Bacon dog Treats", 9.99))
                        .vector(0d, 0d, 0d, 1d, 0d, 0d, 1d, 0d, 0d, 0d, 0d, 0d, 1d, 1d)
                        .build(),
                JsonDocument.builder()
                        .id("pf7044")
                        .document(new Product("PupperSausage Beef dog Treats", 9.99))
                        .vector(0d, 0d, 0d, 1d, 0d, 1d, 1d, 0d, 0d, 0d, 0d, 0d, 1d, 0d)
                        .build());
    }

    @Test
    @Order(3)
    @DisplayName("03. Count Records")
    public void shouldCountRecords() {

         // Default Count
         myCollection.countDocuments();

         // Count with a Filter
         myCollection.countDocuments(new JsonFilter()
             .where("product_price")
              .isEqualsTo(9.99));
    }

    @Test
    @Order(4)
    @DisplayName("04. Similarity Search")
    public void shouldSimilaritySearch() {
        JsonDocumentsClient myCollection = jsonApi
                .namespace(NAMESPACE)
                .collection(COLLECTION_VECTOR);

        Page<JsonDocumentResult> page = myCollection.find(Query.builder()
                // Projection
                .selectVector()
                .selectSimilarity()
                // ann search
                .orderByAnn(1d, 1d, 1d, 1d, 1d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d)
                .limit(2)
                .build());

        System.out.println("Result Size=" + page.getPageSize());
        for(JsonDocumentResult result : page.getResults()) {
            System.out.println(result.getId() + ") similarity=" + result.getSimilarity() + ", vector=" + result.getVector());
        }
    }

    @Test
    @Order(5)
    @DisplayName("05. Meta Data Filtering")
    public void shouldMetaDataFiltering() {

            Page<JsonDocumentResult> page =  myCollection.find(Query.builder()
                    .selectVector()
                    .selectSimilarity()
                    .where("product_price").isEqualsTo(9.99)
                    .orderByAnn(1d, 1d, 1d, 1d, 1d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d)
                    .limit(2)
                    .build());

            System.out.println("Result Size=" + page.getPageSize());
            for(JsonDocumentResult result : page.getResults()) {
                System.out.println(result.getId() + ") similarity=" + result.getSimilarity() + ", vector=" + result.getVector());
            }
    }

}
