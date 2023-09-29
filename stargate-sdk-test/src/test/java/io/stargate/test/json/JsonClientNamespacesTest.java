package io.stargate.test.json;

import io.stargate.sdk.json.StargateJsonApiClient;
import io.stargate.sdk.test.json.AbstractJsonClientNamespacesTest;
import org.junit.jupiter.api.BeforeAll;

/**
 * Default settings (localhost:8180).
 */
public class JsonClientNamespacesTest extends AbstractJsonClientNamespacesTest {
    @BeforeAll
    public static void initStargateRestApiClient() {
        stargateJsonApiClient = new StargateJsonApiClient();
        stargateJsonApiClient.dropNamespace(TEST_NAMESPACE_1);
        stargateJsonApiClient.dropNamespace(TEST_NAMESPACE_2);
    }
}
