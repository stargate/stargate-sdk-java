package io.stargate.sdk.data.test.integration;

import io.stargate.sdk.data.client.DataApiClient;
import io.stargate.sdk.data.client.DataApiClients;
import io.stargate.sdk.data.client.DataApiCollection;
import io.stargate.sdk.data.client.DataApiNamespace;
import io.stargate.sdk.data.client.model.CreateCollectionOptions;
import io.stargate.sdk.data.client.model.Document;
import io.stargate.sdk.data.client.model.SimilarityMetric;
import io.stargate.sdk.data.internal.model.ApiResponse;
import io.stargate.sdk.data.test.TestConstants;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests against a Local Instance of Stargate.
 */
class DataApiNamespaceITTest extends AbstractNamespaceITTest {

    @Override
    public DataApiNamespace initNamespace() {
        return DataApiClients.create().createNamespace(NAMESPACE_NS1);
    }

}
