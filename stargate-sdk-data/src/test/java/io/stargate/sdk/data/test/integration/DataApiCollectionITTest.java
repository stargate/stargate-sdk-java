package io.stargate.sdk.data.test.integration;

import io.stargate.sdk.data.client.DataApiClients;
import io.stargate.sdk.data.client.DataApiNamespace;
import io.stargate.sdk.data.client.observer.LoggerCommandObserver;

/**
 * Allow to test Collection information.
 */
class DataApiCollectionITTest extends AbstractCollectionITTest {

    /** {@inheritDoc} */
    @Override
    protected DataApiNamespace initNamespace() {
        DataApiNamespace apiNameSpace = DataApiClients.create().createNamespace(NAMESPACE_NS1);
        apiNameSpace.registerListener("logger", new LoggerCommandObserver(DataApiNamespace.class));
        return apiNameSpace;
    }

}
