package io.stargate.sdk.test.json;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.stargate.sdk.json.JsonDocumentsClient;
import io.stargate.sdk.json.StargateJsonApiClient;
import io.stargate.sdk.json.domain.CollectionDefinition;
import io.stargate.sdk.json.domain.JsonDocument;
import io.stargate.sdk.json.domain.NamespaceDefinition;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static io.stargate.sdk.json.domain.CollectionDefinition.LLMProvider.openai;
import static io.stargate.sdk.json.domain.CollectionDefinition.SimilarityMetric.cosine;

/**
 * This class test the data api for Keyspaces
 *
 * @author Cedrick LUNVEN (@clunven)
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class AbstractJsonClientNamespacesTest {

    public static final String TEST_NAMESPACE_1 = "ns1";
    public static final String TEST_NAMESPACE_2 = "ns2";
    public static final String TEST_COLLECTION = "col1";
    public static final String TEST_COLLECTION_VECTOR = "vector1";
    public static final String TEST_COLLECTION_VECTORIZE = "vectorize1";

    /** Tested Store. */
    protected static StargateJsonApiClient stargateJsonApiClient;

    /**
     * createNamespace1()
     */
    @Test
    @Order(1)
    @DisplayName("01.Create a simple namespace")
    public void shouldCreateNameSpace() {
        stargateJsonApiClient.createNamespace(TEST_NAMESPACE_1);
        Assertions.assertTrue(stargateJsonApiClient.findNamespaces()
                .collect(Collectors.toSet())
                .contains(TEST_NAMESPACE_1));
    }

    /**
     * createNamespace1()
     */
    @Test
    @Order(2)
    @DisplayName("02.Create a namespace with replication")
    public void shouldCreateNameSpaceWithReplication() {
        stargateJsonApiClient.createNamespace(NamespaceDefinition.builder()
                .name(TEST_NAMESPACE_2)
                .replicationStrategy(NamespaceDefinition.ReplicationStrategy.SimpleStrategy)
                .withOption("replication_factor", 1)
                .build());
        Assertions.assertTrue(stargateJsonApiClient.findNamespaces()
                .collect(Collectors.toSet())
                .contains(TEST_NAMESPACE_2));
    }

    /**
     * findNamespaces()
     */
    @Test
    @Order(3)
    @DisplayName("03.Get all Namespaces")
    public void shouldListNamespace() {
        Set<String> ns = stargateJsonApiClient.findNamespaces().collect(Collectors.toSet());
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
        Assertions.assertTrue(stargateJsonApiClient
                .findNamespaces()
                .collect(Collectors.toSet())
                .contains(TEST_NAMESPACE_2));
        stargateJsonApiClient.dropNamespace(TEST_NAMESPACE_2);
        Assertions.assertFalse(stargateJsonApiClient
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
        stargateJsonApiClient.namespace(TEST_NAMESPACE_1)
                .createCollection(TEST_COLLECTION);
        Assertions.assertTrue(stargateJsonApiClient
                .namespace(TEST_NAMESPACE_1).findCollections()
                .collect(Collectors.toSet())
                .contains(TEST_COLLECTION));
    }

    /**
     * createNamespace1()
     */
    @Test
    @Order(6)
    @DisplayName("06.Create collection with Vector")
    public void shouldCreateCollectionWithVector() {
        stargateJsonApiClient.namespace(TEST_NAMESPACE_1)
                .createCollection(CollectionDefinition.builder()
                        .name(TEST_COLLECTION_VECTOR)
                        .vector(14, cosine)
                        .build());
        Assertions.assertTrue(stargateJsonApiClient
                .namespace(TEST_NAMESPACE_1).findCollections()
                .collect(Collectors.toSet())
                .contains(TEST_COLLECTION_VECTOR));
    }

    /**
     * createNamespace1()
     */
    public void shouldCreateCollectionVectorize() {
        stargateJsonApiClient.namespace(TEST_NAMESPACE_1)
                .createCollection(CollectionDefinition.builder()
                        .name(TEST_COLLECTION_VECTORIZE)
                        .vector(14, cosine)
                        .vectorize(openai, "gpt3.5-turbo")
                        .build());
        Assertions.assertTrue(stargateJsonApiClient
                .namespace(TEST_NAMESPACE_1).findCollections()
                .collect(Collectors.toSet())
                .contains(TEST_COLLECTION_VECTORIZE));
    }

    /**
     * createNamespace1()
     */
    @Test
    @Order(8)
    @DisplayName("08.Drop a collection")
    public void shouldDropCollection() {

        Assertions.assertTrue(stargateJsonApiClient
                .namespace(TEST_NAMESPACE_1).findCollections()
                .collect(Collectors.toSet())
                .contains(TEST_COLLECTION));

        stargateJsonApiClient.namespace(TEST_NAMESPACE_1)
                .dropCollection(TEST_COLLECTION);

        Assertions.assertFalse(stargateJsonApiClient
                .namespace(TEST_NAMESPACE_1)
                .findCollections()
                .collect(Collectors.toSet())
                .contains(TEST_COLLECTION));
    }

    /**
     * createNamespace1()
     */
    @Test
    @Order(9)
    @DisplayName("09.Inserting few documents")
    public void shouldInsertDocuments() {
        Assertions.assertTrue(stargateJsonApiClient
                .namespace(TEST_NAMESPACE_1).findCollections()
                .collect(Collectors.toSet())
                .contains(TEST_COLLECTION_VECTOR));

        JsonDocumentsClient myCollection = stargateJsonApiClient
                .namespace(TEST_NAMESPACE_1)
                .collection(TEST_COLLECTION_VECTOR);
        myCollection.insert(
            JsonDocument.builder()
                .id("pf1844")
                .document(new Product("HealthyFresh - Beef raw dog food"))
                .vector(1d, 0d, 1d, 1d, 1d, 1d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d)
                .build(),
            new JsonDocument("pt0021")
                .jsonDocument("{ \"product_name\": \"Dog Tennis Ball Toy\" }")
                .vector(0d, 0d, 0d, 1d, 0d, 0d, 0d, 0d, 0d, 1d, 1d, 1d, 0d, 0d),
            JsonDocument.builder()
                .id("pf1843")
                .document(Map.of("product_name", "HealthyFresh - Chicken raw dog food"))
                .vector(1d, 1d, 1d, 1d, 1d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d)
                .build(),
            new JsonDocument("pt0041")
                .put("product_name", "Dog Ring Chew Toy")
                .vector(0d, 0d, 0d, 1d, 0d, 0d, 0d, 1d, 1d, 1d, 0d, 0d, 0d, 0d),
            JsonDocument.builder()
                .id("pf7043")
                .document(new Product("PupperSausage Bacon dog Treats"))
                .vector(0d, 0d, 0d, 1d, 0d, 0d, 1d, 0d, 0d, 0d, 0d, 0d, 1d, 1d)
                .build(),
            JsonDocument.builder()
                .id("pf7044")
                .document(new Product("PupperSausage Beef dog Treats"))
                .vector(0d, 0d, 0d, 1d, 0d, 1d, 1d, 0d, 0d, 0d, 0d, 0d, 1d, 0d)
                .build());
    }

    /**
     * createNamespace1()
     */
    @Test
    @Order(10)
    @DisplayName("10.Count documents")
    public void shouldCountDocuments() {
        Assertions.assertTrue(stargateJsonApiClient
                .namespace(TEST_NAMESPACE_1).findCollections()
                .collect(Collectors.toSet())
                .contains(TEST_COLLECTION_VECTOR));
        Assertions.assertEquals(6, stargateJsonApiClient
                .namespace(TEST_NAMESPACE_1)
                .collection(TEST_COLLECTION_VECTOR)
                .countDocuments());
    }


    // ---- Tests POJO --

    private static class Product {
        @JsonProperty("product_name")
        private String name;
        public Product(String name) {this.name = name;}
        public String getName() { return name; }
    }



}
