package io.stargate.sdk.data.test.integration;

import io.stargate.sdk.data.client.DataApiClients;
import io.stargate.sdk.data.client.Database;
import io.stargate.sdk.data.client.observer.LoggerCommandObserver;

/**
 * Integration tests against a Local Instance of Stargate.
 */
class DatabaseITTest extends AbstractDatabaseITTest {

    /** {@inheritDoc} */
    @Override
    protected Database initNamespace() {
        Database apiNameSpace = DataApiClients.create().createNamespace(NAMESPACE_NS1);
        apiNameSpace.registerListener("logger", new LoggerCommandObserver(Database.class));
        return apiNameSpace;
    }

}
