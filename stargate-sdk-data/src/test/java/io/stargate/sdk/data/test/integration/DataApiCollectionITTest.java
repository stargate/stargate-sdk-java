package io.stargate.sdk.data.test.integration;

import io.stargate.sdk.data.client.DataApiClients;
import io.stargate.sdk.data.client.DataApiNamespace;

/**
 * Allow to test Collection information.
 */
public class DataApiCollectionITTest extends AbstractCollectionITTest {

    @Override
    public DataApiNamespace initNamespace() {
        return DataApiClients.create().createNamespace(NAMESPACE_NS1);
    }

}
