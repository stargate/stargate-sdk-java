package io.stargate.sdk.data.test.integration;

import io.stargate.sdk.data.client.DataApiClients;
import io.stargate.sdk.data.client.DataApiNamespace;
import io.stargate.sdk.data.client.observer.LoggerCommandObserver;

/**
 * Integration tests against a Local Instance of Stargate.
 */
class DataApiNamespaceITTest extends AbstractNamespaceITTest {

    /** {@inheritDoc} */
    @Override
    protected DataApiNamespace initNamespace() {
        DataApiNamespace apiNameSpace = DataApiClients.create().createNamespace(NAMESPACE_NS1);
        apiNameSpace.registerListener("logger", new LoggerCommandObserver(DataApiNamespace.class));
        return apiNameSpace;
    }

}
