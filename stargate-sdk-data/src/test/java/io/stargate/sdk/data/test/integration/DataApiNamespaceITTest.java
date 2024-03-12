package io.stargate.sdk.data.test.integration;

import io.stargate.sdk.data.client.DataApiClients;
import io.stargate.sdk.data.client.DataApiNamespace;

/**
 * Integration tests against a Local Instance of Stargate.
 */
class DataApiNamespaceITTest extends AbstractNamespaceITTest {

    @Override
    public DataApiNamespace initNamespace() {
        return DataApiClients.create().createNamespace(NAMESPACE_NS1);
    }

}
