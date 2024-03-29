package io.stargate.sdk.test.json;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.stargate.sdk.data.DataApiClient;
import io.stargate.sdk.data.CollectionClient;
import io.stargate.sdk.data.NamespaceClient;
import io.stargate.sdk.data.domain.CollectionDefinition;
import io.stargate.sdk.data.domain.JsonDocument;
import io.stargate.sdk.data.domain.NamespaceDefinition;
import io.stargate.sdk.data.domain.odm.Document;
import io.stargate.sdk.data.domain.query.Filter;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static io.stargate.sdk.data.domain.SimilarityMetric.cosine;

/**
 * This class test the data api for Keyspaces
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class AbstractJsonClientNamespacesTest {

    /** test constants. */
    public static final String TEST_NAMESPACE_1 = "ns1";
    /** test constants. */
    public static final String TEST_NAMESPACE_2 = "ns2";
    /** test constants. */
    public static final String TEST_COLLECTION = "col1";
    /** test constants. */
    public static final String TEST_COLLECTION_VECTOR = "vector1";
    /** test constants. */
    public static final String TEST_COLLECTION_VECTORIZE = "vectorize1";

    /** Tested Store. */
    protected static DataApiClient jsonApiClient;
    /** Tested Namespace. */
    protected static NamespaceClient nsClient;

    /**
     * Default constructor.
     */
    public AbstractJsonClientNamespacesTest() {
    }

    /**
     * shouldCreateNameSpace()
     */
    @Test
    @Order(1)
    @DisplayName("01.Create a simple namespace")
    public void shouldCreateNameSpace() {
        // Given
        Assertions.assertFalse(jsonApiClient.isNamespaceExists(TEST_NAMESPACE_1));
        // When
        jsonApiClient.createNamespace(TEST_NAMESPACE_1);
        // Then
        Assertions.assertTrue(jsonApiClient.isNamespaceExists(TEST_NAMESPACE_1));
        nsClient = jsonApiClient.namespace(TEST_NAMESPACE_1);
    }

    /**
     * shouldCreateNameSpaceWithReplication()
     */
    @Test
    @Order(2)
    @DisplayName("02.Create a namespace with replication")
    public void shouldCreateNameSpaceWithReplication() {
        // Given
        Assertions.assertFalse(jsonApiClient.isNamespaceExists(TEST_NAMESPACE_2));
        // When
        jsonApiClient.createNamespace(NamespaceDefinition.builder()
                .name(TEST_NAMESPACE_2)
                .replicationStrategy(NamespaceDefinition.ReplicationStrategy.SimpleStrategy)
                .withOption("replication_factor", 1)
                .build());
        // Then
        Assertions.assertTrue(jsonApiClient.isNamespaceExists(TEST_NAMESPACE_2));
    }

    /**
     * shouldListNamespace()
     */
    @Test
    @Order(3)
    @DisplayName("03.Get all Namespaces")
    public void shouldListNamespace() {
        Set<String> ns = jsonApiClient.findAllNamespaces().collect(Collectors.toSet());
        Assertions.assertTrue(ns.contains(TEST_NAMESPACE_1));
        Assertions.assertTrue(ns.contains(TEST_NAMESPACE_2));
    }

    /**
     * shouldDropNamespace()
     */
    @Test
    @Order(4)
    @DisplayName("04.Drop namespace")
    public void shouldDropNamespace() {
        Assertions.assertTrue(jsonApiClient
                .findAllNamespaces()
                .collect(Collectors.toSet())
                .contains(TEST_NAMESPACE_2));
        jsonApiClient.dropNamespace(TEST_NAMESPACE_2);
        Assertions.assertFalse(jsonApiClient
                .findAllNamespaces()
                .collect(Collectors.toSet())
                .contains(TEST_NAMESPACE_2));
    }

    /**
     * shouldCreateCollection()
     */
    @Test
    @Order(5)
    @DisplayName("05.Create simple collection")
    public void shouldCreateCollection() {
        // Given
        Assertions.assertFalse(nsClient.isCollectionExists(TEST_COLLECTION));
        // When
        nsClient.createCollection(TEST_COLLECTION);
        // Then
        Assertions.assertTrue(nsClient.isCollectionExists(TEST_COLLECTION));
    }

    /**
     * shouldCreateCollectionWithVector()
     */
    @Test
    @Order(6)
    @DisplayName("06.Create collection with Vector")
    public void shouldCreateCollectionWithVector() {
        // Given
        Assertions.assertFalse(nsClient.isCollectionExists(TEST_COLLECTION_VECTOR));
        // when
        jsonApiClient.namespace(TEST_NAMESPACE_1)
                .createCollection(CollectionDefinition.builder()
                        .name(TEST_COLLECTION_VECTOR)
                        .vector(14, cosine)
                        .build());
        jsonApiClient.namespace(TEST_NAMESPACE_1)
                .createCollection(CollectionDefinition.builder()
                        .name("tmp_vector")
                        .vector(14, cosine)
                        .build());
        // Then
        Assertions.assertTrue(nsClient.isCollectionExists(TEST_COLLECTION_VECTOR));
        Assertions.assertTrue(nsClient.isCollectionExists("tmp_vector"));
    }

    /**
     * shouldCreateCollectionVectorize
     */
    @Test
    @Order(7)
    @Disabled
    @DisplayName("07.Create collection with Vectorize")
    public void shouldCreateCollectionVectorize() {
      // Given
      Assertions.assertFalse(nsClient.isCollectionExists(TEST_COLLECTION_VECTORIZE));
      // When
      nsClient.createCollection(CollectionDefinition.builder()
              .name(TEST_COLLECTION_VECTORIZE)
              .vector(14, cosine)
              //.vectorize("openai", "gpt3.5-turbo")
              .build());
       // Then
       Assertions.assertTrue(nsClient.isCollectionExists(TEST_COLLECTION_VECTORIZE));
       nsClient.createCollection(CollectionDefinition.builder()
                .name("tmp_vectorize")
                .vector(14, cosine)
                //.vectorize("openai", "gpt3.5-turbo")
                .build());
        //Assertions.assertTrue(nsClient.isCollectionExists("tmp_vectorize"));
    }

    /**
     * shouldDropCollection()
     */
    @Test
    @Order(8)
    @DisplayName("08.Drop a collection")
    public void shouldDropCollection() {
        // Given
        Assertions.assertTrue(nsClient.isCollectionExists("tmp_vector"));
        // When
        nsClient.deleteCollection("tmp_vector");
        // Then
        Assertions.assertFalse(nsClient.isCollectionExists("tmp_vector"));
    }

    /**
     * shouldInsertOne()
     */
    @Test
    @Order(9)
    @DisplayName("09.Insert One")
    public void shouldInsertOne() {
        NamespaceClient ns = jsonApiClient.namespace(TEST_NAMESPACE_1);
        ns.createCollection(TEST_COLLECTION);

        // Insert with a Json record => KV access
        CollectionClient col = ns.collection(TEST_COLLECTION);

        // Insert a random object (id is generated) with or without id (will be generated)
        Assertions.assertNotNull(col.insertOne(
                new Document<Product>()
                        .data(new Product("something Good", 9.99))));
        Assertions.assertEquals("id1", col.insertOne(
                new Document<>().id("id1").data(new Product("something Good", 10.99))));

        // Insert a Json String, will do the needed magic
        Assertions.assertEquals("id2", col
                .insertOne(new JsonDocument().id("id2").data("{\"key\": \"value\"}")));

        // Insert a Map, also do the magic
        col.insertOne(new JsonDocument().id("id3").data("{\"key\": \"value\"}"));

        // fine-grained json
        col.insertOne(new JsonDocument().id("pf1844").put("attribute", "test"));
    }

    /**
     * shouldInsertOneVector
     */
    @Test
    @Order(10)
    @DisplayName("10.Insert One Vector")
    public void shouldInsertOneVector() {
        CollectionClient colVector = jsonApiClient
                .namespace(TEST_NAMESPACE_1)
                .createCollection(TEST_COLLECTION_VECTOR, 14);

        // Add vector with an id
        colVector.insertOne(new Document<>(
                "product1",
                new Product("something Good", 9.99),
                new float[] {1f, 0f, 1f, 1f, 1f, 1f, 0f, 0f, 0f, 0f, 0f, 0f, 0f, 0f}));

        // Add vector without an id
        colVector.insertOne(new Document<>()
                .data(new Product("id will be generated for you", 10.99))
                .vector(new float[] {1f, 0f, 1f, 1f, 1f, 1f, 0f, 0f, 0f, 0f, 0f, 0f, 0f, 0f}));

        // Insert a full-fledged object
        colVector.insertOne(new JsonDocument()
                .id("pf2000")
                .put("attribute", "test")
                .vector(new float[] {1f, 0f, 1f, 1f, 1f, 1f, 0f, 0f, 0f, 0f, 0f, 0f, 0f, 0f}));
    }

    /**
     * shouldInsertMany()
     */
    @Test
    @Order(11)
    @DisplayName("11.Inserting many")
    public void shouldInsertMany() {
        Assertions.assertTrue(jsonApiClient
                .namespace(TEST_NAMESPACE_1)
                .findCollections()
                .map(CollectionDefinition::getName)
                .collect(Collectors.toSet())
                .contains(TEST_COLLECTION_VECTOR));

        CollectionClient myCollection = jsonApiClient
                .namespace(TEST_NAMESPACE_1)
                .collection(TEST_COLLECTION_VECTOR);

        /*
        myCollection.insertMany(List.of(
                new Document<Product>()
                        .id("pf1844")
                        .data(new Product("HealthyFresh - Beef raw dog food", 9.99))
                        .vector(new float[] {1f, 0f, 1f, 1f, 1f, 1f, 0f, 0f, 0f, 0f, 0f, 0f, 0f, 0f}),
                new JsonDocument("pt0021")
                        .data("{ \"product_name\": \"Dog Tennis Ball Toy\" }")
                        .vector(new float[] {0f, 0f, 0f, 1f, 0f, 0f, 0f, 0f, 0f, 1f, 1f, 1f, 0f, 0f}),
                new JsonDocument()
                        .id("pf1843")
                        .data(Map.of("product_name", "HealthyFresh - Chicken raw dog food"))
                        .vector(new float[] {1f, 1f, 1f, 1f, 1f, 0f, 0f, 0f, 0f, 0f, 0f, 0f, 0f, 0f}),
                new JsonDocument("pt0041")
                        .put("product_name", "Dog Ring Chew Toy")
                        .vector(new float[] {0f, 0f, 0f, 1f, 0f, 0f, 0f, 1f, 1f, 1f, 0f, 0f, 0f, 0f}),
                new Document<Product>().id("pf7043")
                        .data(new Product("Pepper Sausage Bacon dog Treats", 9.99))
                        .vector(new float[] {0f, 0f, 0f, 1f, 0f, 0f, 1f, 0f, 0f, 0f, 0f, 0f, 1f, 1f}),
                new Document<Product>()
                        .id("pf7044")
                        .data(new Product("Pepper Sausage Beef dog Treats", 10.99))
                        .vector(new float[] {0f, 0f, 0f, 1f, 0f, 1f, 1f, 0f, 0f, 0f, 0f, 0f, 1f, 0f}))
        );*/

    }

    /**
     * shouldCountDocuments()
     */
    @Test
    @Order(12)
    @DisplayName("12.Count")
    public void shouldCountDocuments() {
        // Given
        Assertions.assertTrue(nsClient.isCollectionExists(TEST_COLLECTION_VECTOR));
        Assertions.assertTrue(nsClient.isCollectionExists(TEST_COLLECTION));
        // WHen
        Assertions.assertEquals(10, nsClient.collection(TEST_COLLECTION_VECTOR).countDocuments());
        Assertions.assertEquals(7, nsClient.collection(TEST_COLLECTION).countDocuments());
        Assertions.assertEquals(3, nsClient.collection(TEST_COLLECTION_VECTOR)
                .countDocuments(new Filter()
                .where("product_price")
                .isEqualsTo(9.99)));
    }


    // ---- Tests POJO --

    /**
     * Pojo
     */
    @Getter @AllArgsConstructor
    protected static class Product {
        /** name. */
        @JsonProperty("product_name")
        private String name;
        /** price. */
        @JsonProperty("product_price")
        private Double price;
        /** Default constructor. */
        public Product() {}
    }

}
