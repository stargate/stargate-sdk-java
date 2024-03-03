package io.stargate.data_api.client;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;

/**
 * Test operations on namespaces
 */
public class DataApiNamespaceTestIT {

    /** test constants. */
    public static final String TEST_NAMESPACE_1 = "ns1";

    /** test constants. */
    public static final String TEST_NAMESPACE_2 = "ns2";

    /** Tested Store. */
    protected static DataApiClient jsonApiClient;
    /** Tested Namespace. */
    protected static DataApiNamespace nsClient;

    @BeforeAll
    public static void initStargateRestApiClient() {
        jsonApiClient = DataApiClients.create();
        jsonApiClient.dropNamespace(TEST_NAMESPACE_1);
        jsonApiClient.dropNamespace(TEST_NAMESPACE_2);
    }

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
        nsClient = jsonApiClient.getNamespace(TEST_NAMESPACE_1);
    }


}
