package io.stargate.data_api.client;

import io.stargate.data_api.client.model.CreateNamespaceOptions;
import io.stargate.sdk.ServiceDatacenter;
import io.stargate.sdk.ServiceDeployment;
import io.stargate.sdk.api.TokenProvider;
import io.stargate.sdk.http.ServiceHttp;
import io.stargate.sdk.http.auth.TokenProviderHttpAuth;
import io.stargate.sdk.utils.AnsiUtils;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Map;
import java.util.stream.Collectors;

import static io.stargate.data_api.internal.DataApiClientImpl.DEFAULT_ENDPOINT;

/**
 * Test connectivity to API
 */
@Slf4j
public class DataApiClientTestIT {

    @Test
    public void shouldConnectWithDefault() {
        // Given, auth service is started
        String token = new TokenProviderHttpAuth().getToken();
        log.info("Sample Token [" + AnsiUtils.yellow(token) + "]");
        Assertions.assertNotNull(token);
        // When
        DataApiClient apiClient = DataApiClients.create();
        // Then
        Assertions.assertTrue(apiClient.listNamespaceNames().count() > 0);
        log.info(""+apiClient.listNamespaceNames().collect(Collectors.toList()));
    }

    @Test
    public void shouldConnectWithEndpointAndToken() {
        // Given, auth service is started
        String token = new TokenProviderHttpAuth().getToken();
        log.info("Sample Token [" + AnsiUtils.yellow(token) + "]");
        Assertions.assertNotNull(token);
        // When
        DataApiClient apiClient = DataApiClients.create(DEFAULT_ENDPOINT, token);
        // Then
        Assertions.assertTrue(apiClient.listNamespaceNames().count() > 0);
        log.info(""+apiClient.listNamespaceNames().collect(Collectors.toList()));
    }

    @Test
    public void shouldConnectWithServiceDeployment() {
        // Given
        TokenProvider tokenProvider =
                new TokenProviderHttpAuth("cassandra", "cassandra");
        log.info("Sample Token [" + AnsiUtils.yellow(tokenProvider.getToken()) + "]");

        // Node
        ServiceHttp endpoint = new ServiceHttp("demo",
                "http://localhost:8181",
                "http://localhost:8181/v1/stargate/health");
        // DC with default auth and single node
        ServiceDatacenter<ServiceHttp> sDc =
                new ServiceDatacenter<>("dc", tokenProvider, Collections.singletonList(endpoint));
        // Deployment with a single dc
        // When
        DataApiClient apiClient = DataApiClients.create(new ServiceDeployment<ServiceHttp>().addDatacenter(sDc));
        // Then
        Assertions.assertTrue(apiClient.listNamespaceNames().count() > 0);
        log.info(""+apiClient.listNamespaceNames().collect(Collectors.toList()));
    }

    @Test
    public void shouldCreateNamespaces() {

        // Default Settings
        DataApiClient apiClient = DataApiClients.create();

        // Default
        DataApiNamespace ns1 = apiClient.createNamespace("ns1");
        Assertions.assertNotNull(ns1);

        // SimpleStrategy
        DataApiNamespace ns2 = apiClient.createNamespace("ns2",
                CreateNamespaceOptions.simpleStrategy(1));

        // Network Strategy
        DataApiNamespace ns3 = apiClient.createNamespace("ns3",
                CreateNamespaceOptions.networkTopologyStrategy(Map.of("datacenter1", 1)));

    }


}
