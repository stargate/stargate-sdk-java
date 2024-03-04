package io.stargate.data_api.client;

import io.stargate.data_api.client.model.CreateCollectionOptions;
import io.stargate.data_api.client.model.Document;
import io.stargate.data_api.client.model.SimilarityMetric;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;

import java.util.stream.Collectors;

/**
 * Test operations on namespaces
 */
public class DataApiNamespaceTestIT {

    /** test constants. */
    public static final String TEST_NAMESPACE_1 = "ns1";


    /** Tested Store. */
    protected static DataApiClient apiClient;
    /** Tested Namespace. */
    protected static DataApiNamespace namespace;

    @BeforeAll
    public static void initStargateRestApiClient() {
        apiClient = DataApiClients.create();
        Assertions.assertNotNull(apiClient);
        namespace = apiClient.createNamespace(TEST_NAMESPACE_1);
        Assertions.assertNotNull(namespace);
    }

    @Test
    public void shouldCreateCollection() {
        // When
        Assertions.assertEquals(TEST_NAMESPACE_1, namespace.getName());

        System.out.println(namespace.listCollectionNames().collect(Collectors.toList()));
        System.out.println(namespace.listCollections().collect(Collectors.toList()));

        DataApiCollection<Document> col1 = namespace.createCollection("collection_simple");
        DataApiCollection<Document> col2 = namespace
                .createCollection("collection_vector", CreateCollectionOptions.builder()
                        .withVectorDimension(14)
                        .withVectorSimilarityMetric(SimilarityMetric.cosine)
                        .withIndexingDeny("body")
                        .build());
        DataApiCollection<Document> col3 = namespace
                .createCollectionVector("collection_vector2", 14, SimilarityMetric.cosine);
    }


}
