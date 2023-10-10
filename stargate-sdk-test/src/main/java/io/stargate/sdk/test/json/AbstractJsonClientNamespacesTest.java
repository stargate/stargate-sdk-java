package io.stargate.sdk.test.json;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.stargate.sdk.json.JsonApiClient;
import io.stargate.sdk.json.JsonCollectionClient;
import io.stargate.sdk.json.JsonNamespaceClient;
import io.stargate.sdk.json.domain.CollectionDefinition;
import io.stargate.sdk.json.domain.Filter;
import io.stargate.sdk.json.domain.JsonRecord;
import io.stargate.sdk.json.domain.NamespaceDefinition;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static io.stargate.sdk.json.vector.SimilarityMetric.cosine;

/**
 * This class test the data api for Keyspaces
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class AbstractJsonClientNamespacesTest {

    public static final String TEST_NAMESPACE_1 = "ns1";
    public static final String TEST_NAMESPACE_2 = "ns2";
    public static final String TEST_COLLECTION = "col1";
    public static final String TEST_COLLECTION_VECTOR = "vector1";
    public static final String TEST_COLLECTION_VECTORIZE = "vectorize1";

    /** Tested Store. */
    protected static JsonApiClient jsonApiClient;

    protected static JsonNamespaceClient nsClient;

    /**
     * createNamespace1()
     */
    @Test
    @Order(1)
    @DisplayName("01.Create a simple namespace")
    public void shouldCreateNameSpace() {
        // Given
        Assertions.assertFalse(jsonApiClient.existNamespace(TEST_NAMESPACE_1));
        // When
        jsonApiClient.createNamespace(TEST_NAMESPACE_1);
        // Then
        Assertions.assertTrue(jsonApiClient.existNamespace(TEST_NAMESPACE_1));
        nsClient = jsonApiClient.namespace(TEST_NAMESPACE_1);
    }

    /**
     * createNamespace1()
     */
    @Test
    @Order(2)
    @DisplayName("02.Create a namespace with replication")
    public void shouldCreateNameSpaceWithReplication() {
        // Given
        Assertions.assertFalse(jsonApiClient.existNamespace(TEST_NAMESPACE_2));
        // When
        jsonApiClient.createNamespace(NamespaceDefinition.builder()
                .name(TEST_NAMESPACE_2)
                .replicationStrategy(NamespaceDefinition.ReplicationStrategy.SimpleStrategy)
                .withOption("replication_factor", 1)
                .build());
        // Then
        Assertions.assertTrue(jsonApiClient.existNamespace(TEST_NAMESPACE_2));
    }

    /**
     * findNamespaces()
     */
    @Test
    @Order(3)
    @DisplayName("03.Get all Namespaces")
    public void shouldListNamespace() {
        Set<String> ns = jsonApiClient.findNamespaces().collect(Collectors.toSet());
        Assertions.assertTrue(ns.contains(TEST_NAMESPACE_1));
        Assertions.assertTrue(ns.contains(TEST_NAMESPACE_2));
    }


    /**
     * findNamespaces()
     */
    @Test
    @Order(4)
    @DisplayName("04.Drop namespace")
    public void shouldDropNamespace() {
        Assertions.assertTrue(jsonApiClient
                .findNamespaces()
                .collect(Collectors.toSet())
                .contains(TEST_NAMESPACE_2));
        jsonApiClient.dropNamespace(TEST_NAMESPACE_2);
        Assertions.assertFalse(jsonApiClient
                .findNamespaces()
                .collect(Collectors.toSet())
                .contains(TEST_NAMESPACE_2));
    }

    /**
     * createNamespace1()
     */
    @Test
    @Order(5)
    @DisplayName("05.Create simple collection")
    public void shouldCreateCollection() {
        // Given
        Assertions.assertFalse(nsClient.existCollection(TEST_COLLECTION));
        // When
        nsClient.createCollection(TEST_COLLECTION);
        // Then
        Assertions.assertTrue(nsClient.existCollection(TEST_COLLECTION));
    }

    /**
     * createNamespace1()
     */
    @Test
    @Order(6)
    @DisplayName("06.Create collection with Vector")
    public void shouldCreateCollectionWithVector() {
        // Given
        Assertions.assertFalse(nsClient.existCollection(TEST_COLLECTION_VECTOR));
        // when
        jsonApiClient.namespace(TEST_NAMESPACE_1)
                .createCollection(CollectionDefinition.builder()
                        .name(TEST_COLLECTION_VECTOR)
                        .vector(14, cosine)
                        .build());
        jsonApiClient.namespace(TEST_NAMESPACE_1)
                .createCollectionVector("tmp_vector", 14, cosine);
        // Then
        Assertions.assertTrue(nsClient.existCollection(TEST_COLLECTION_VECTOR));
        Assertions.assertTrue(nsClient.existCollection("tmp_vector"));
    }

    @Test
    @Order(7)
    @Disabled
    @DisplayName("07.Create collection with Vectorize")
    public void shouldCreateCollectionVectorize() {
      // Given
      Assertions.assertFalse(nsClient.existCollection(TEST_COLLECTION_VECTORIZE));
      // When
      nsClient.createCollection(CollectionDefinition.builder()
              .name(TEST_COLLECTION_VECTORIZE)
              .vector(14, cosine)
              .vectorize("openai", "gpt3.5-turbo")
              .build());
       // Then
       Assertions.assertTrue(nsClient.existCollection(TEST_COLLECTION_VECTORIZE));

       nsClient.createCollectionVector("tmp_vectorize", 14,
                cosine, "openai", "gpt3.5-turbo");

    }

    /**
     * createNamespace1()
     */
    @Test
    @Order(8)
    @DisplayName("08.Drop a collection")
    public void shouldDropCollection() {
        // Given
        Assertions.assertTrue(nsClient.existCollection("tmp_vector"));
        // When
        nsClient.deleteCollection("tmp_vector");
        // Then
        Assertions.assertFalse(nsClient.existCollection("tmp_vector"));
    }

    @Test
    @Order(9)
    @DisplayName("09.Insert One")
    public void shouldInsertOne() {
        JsonNamespaceClient ns = jsonApiClient.namespace(TEST_NAMESPACE_1);
        ns.createCollection(TEST_COLLECTION);

        // Insert with a Json record => KV access
        JsonCollectionClient col = ns.collection(TEST_COLLECTION);

        // Insert a random object (id is generated) with or without id (will be generated)
        Assertions.assertNotNull(col.insertOne(new Product("something Good", 9.99)));
        Assertions.assertEquals("id1", col.insertOne("id1", new Product("something Good", 10.99)));

        // Insert a Json String, will do the needed magic
        col.insertOne("{\"key\": \"value\"}");
        Assertions.assertEquals("id2", col.insertOne("id2", "{\"key\": \"value\"}"));

        // Insert a Map, also do the magic
        col.insertOne(Map.of("anotherKey", 12));
        col.insertOne("id3", "{\"key\": \"value\"}");

        // fine-grained json
        col.insertOne(new JsonRecord("pf1844").put("attribute", "test"));
    }

    @Test
    @Order(10)
    @DisplayName("10.Insert One Vector")
    public void shouldInsertOneVector() {
        JsonNamespaceClient ns = jsonApiClient.namespace(TEST_NAMESPACE_1);
        ns.createCollectionVector(TEST_COLLECTION_VECTOR, 14, cosine);

        // Insert with a Json record => KV access
        JsonCollectionClient colVector = ns.collection(TEST_COLLECTION_VECTOR);

        // work with no vector mapper
        colVector.insertOne(Map.of("anotherKey", 12));

        // Add vector with an id
        colVector.insertOne(
                "product1",
                new Product("something Good", 9.99),
                new float[] {1f, 0f, 1f, 1f, 1f, 1f, 0f, 0f, 0f, 0f, 0f, 0f, 0f, 0f});

        // Add vector without an id
        colVector.insertOne(
                new Product("id will be generated for you", 10.99),
                new float[] {1f, 0f, 1f, 1f, 1f, 1f, 0f, 0f, 0f, 0f, 0f, 0f, 0f, 0f});

        // Insert a full-fledged object
        colVector.insertOne(new JsonRecord()
                .id("pf2000")
                .put("attribute", "test")
                .vector(new float[] {1f, 0f, 1f, 1f, 1f, 1f, 0f, 0f, 0f, 0f, 0f, 0f, 0f, 0f}));
    }

    /**
     * createNamespace1()
     */
    @Test
    @Order(11)
    @DisplayName("11.Inserting many")
    public void shouldInsertMany() {
        Assertions.assertTrue(jsonApiClient
                .namespace(TEST_NAMESPACE_1).findCollections()
                .collect(Collectors.toSet())
                .contains(TEST_COLLECTION_VECTOR));

        JsonCollectionClient myCollection = jsonApiClient
                .namespace(TEST_NAMESPACE_1)
                .collection(TEST_COLLECTION_VECTOR);

        myCollection.insertMany(
                new JsonRecord()
                        .id("pf1844")
                        .data(new Product("HealthyFresh - Beef raw dog food", 9.99))
                        .vector(new float[] {1f, 0f, 1f, 1f, 1f, 1f, 0f, 0f, 0f, 0f, 0f, 0f, 0f, 0f}),
                new JsonRecord("pt0021")
                        .data("{ \"product_name\": \"Dog Tennis Ball Toy\" }")
                        .vector(new float[] {0f, 0f, 0f, 1f, 0f, 0f, 0f, 0f, 0f, 1f, 1f, 1f, 0f, 0f}),
                new JsonRecord()
                        .id("pf1843")
                        .data(Map.of("product_name", "HealthyFresh - Chicken raw dog food"))
                        .vector(new float[] {1f, 1f, 1f, 1f, 1f, 0f, 0f, 0f, 0f, 0f, 0f, 0f, 0f, 0f}),
                new JsonRecord("pt0041")
                        .put("product_name", "Dog Ring Chew Toy")
                        .vector(new float[] {0f, 0f, 0f, 1f, 0f, 0f, 0f, 1f, 1f, 1f, 0f, 0f, 0f, 0f}),
                new JsonRecord("pf7043", new Product("Pepper Sausage Bacon dog Treats", 9.99))
                        .vector(new float[] {0f, 0f, 0f, 1f, 0f, 0f, 1f, 0f, 0f, 0f, 0f, 0f, 1f, 1f}),
                new JsonRecord()
                        .id("pf7044")
                        .data(new Product("Pepper Sausage Beef dog Treats", 10.99))
                        .vector(new float[] {0f, 0f, 0f, 1f, 0f, 1f, 1f, 0f, 0f, 0f, 0f, 0f, 1f, 0f})
        );

    }

    /**
     * createNamespace1()
     */
    @Test
    @Order(12)
    @DisplayName("12.Count")
    public void shouldCountDocuments() {
        // Given
        Assertions.assertTrue(nsClient.existCollection(TEST_COLLECTION_VECTOR));
        Assertions.assertTrue(nsClient.existCollection(TEST_COLLECTION));
        // WHen
        Assertions.assertEquals(10, nsClient.collection(TEST_COLLECTION_VECTOR).countDocuments());
        Assertions.assertEquals(7, nsClient.collection(TEST_COLLECTION).countDocuments());
        Assertions.assertEquals(3, nsClient.collection(TEST_COLLECTION_VECTOR)
                .countDocuments(new Filter()
                .where("product_price")
                .isEqualsTo(9.99)));
    }



    // ---- Tests POJO --

    @Getter @AllArgsConstructor @NoArgsConstructor
    protected static class Product {
        @JsonProperty("product_name")
        private String name;
        @JsonProperty("product_price")
        private Double price;
    }

}
