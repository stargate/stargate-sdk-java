package io.stargate.sdk.test.json;

import io.stargate.sdk.json.StargateJsonApiClient;
import io.stargate.sdk.json.domain.CreateCollectionRequest;
import io.stargate.sdk.json.domain.CreateNamespaceRequest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import java.util.Set;
import java.util.stream.Collectors;

import static io.stargate.sdk.json.domain.CreateCollectionRequest.LLMProvider.openai;
import static io.stargate.sdk.json.domain.CreateCollectionRequest.SimilarityMetric.cosine;

/**
 * This class test the data api for Keyspaces
 *
 * @author Cedrick LUNVEN (@clunven)
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class AbstractJsonClientNamespacesTest {

    public static final String TEST_NAMESPACE_1 = "ns1";
    public static final String TEST_NAMESPACE_2 = "ns2";
    public static final String TEST_COLLECTION_1 = "col1";
    public static final String TEST_COLLECTION_2 = "col2";
    public static final String TEST_COLLECTION_3 = "col3";

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
        stargateJsonApiClient.createNamespace(CreateNamespaceRequest.builder()
                .name(TEST_NAMESPACE_2)
                .replicationStrategy(CreateNamespaceRequest.ReplicationStrategy.SimpleStrategy)
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
                .createCollection(TEST_COLLECTION_1);
        Assertions.assertTrue(stargateJsonApiClient
                .namespace(TEST_NAMESPACE_1).findCollections()
                .collect(Collectors.toSet())
                .contains(TEST_COLLECTION_1));
    }

    /**
     * createNamespace1()
     */
    @Test
    @Order(6)
    @DisplayName("06.Create collection with Vector")
    public void shouldCreateCollectionWithVector() {
        stargateJsonApiClient.namespace(TEST_NAMESPACE_1)
                .createCollection(CreateCollectionRequest.builder()
                        .name(TEST_COLLECTION_2)
                        .vectorDimension(1536)
                        .similarityMetric(cosine)
                        .build());
        Assertions.assertTrue(stargateJsonApiClient
                .namespace(TEST_NAMESPACE_1).findCollections()
                .collect(Collectors.toSet())
                .contains(TEST_COLLECTION_2));
    }

    /**
     * createNamespace1()
     */
    public void shouldCreateCollectionVectorize() {
        stargateJsonApiClient.namespace(TEST_NAMESPACE_1)
                .createCollection(CreateCollectionRequest.builder()
                        .name(TEST_COLLECTION_3)
                        .vectorDimension(1536)
                        .similarityMetric(cosine)
                        .llmProvider(openai)
                        .llmModel("gpt3.5-turbo")
                        .build());

        Assertions.assertTrue(stargateJsonApiClient
                .namespace(TEST_NAMESPACE_1).findCollections()
                .collect(Collectors.toSet())
                .contains(TEST_COLLECTION_3));
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
                .contains(TEST_COLLECTION_1));

        stargateJsonApiClient.namespace(TEST_NAMESPACE_1)
                .dropCollection(TEST_COLLECTION_1);

        Assertions.assertFalse(stargateJsonApiClient
                .namespace(TEST_NAMESPACE_1)
                .findCollections()
                .collect(Collectors.toSet())
                .contains(TEST_COLLECTION_1));
    }

}
