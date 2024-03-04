package io.stargate.data_api.client;

import io.stargate.data_api.client.exception.DataApiException;
import io.stargate.data_api.client.exception.NamespaceNotFoundException;
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

    // --------------------
    // -- Initialization
    // --------------------

    @Test
    public void shouldGetToken() {
        log.info(new TokenProviderHttpAuth().getToken());
    }

    @Test
    public void shouldConnectWithDefault() {
        // When
        DataApiClient apiClient = DataApiClients.create();
        Assertions.assertNotNull(apiClient);
        // Then
        Assertions.assertTrue(apiClient.listNamespaceNames().count() > 0);
    }

    @Test
    public void shouldConnectWithEndpointAndToken() {
        // When
        DataApiClient apiClient = DataApiClients.create(DEFAULT_ENDPOINT, new TokenProviderHttpAuth().getToken());
        Assertions.assertNotNull(apiClient);
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
        // When
        DataApiClient apiClient = DataApiClients.create(new ServiceDeployment<ServiceHttp>().addDatacenter(sDc));
        Assertions.assertNotNull(apiClient);
        // Then
        Assertions.assertTrue(apiClient.listNamespaceNames().findAny().isPresent());
    }

    // --------------------
    // -- create Namespace
    // --------------------

    @Test
    public void shouldCreateNamespaceDefault() {
        // Given
        DataApiClient apiClient = DataApiClients.create();
        // When
        DataApiNamespace ns1 = apiClient.createNamespace("ns1");
        // Then
        Assertions.assertNotNull(ns1);
        Assertions.assertTrue(apiClient.isNamespaceExists("ns1"));
        // Then, no error if namespace already exist
        Assertions.assertNotNull(apiClient.createNamespace("ns1"));

        //apiClient.createNamespace(null);
        Assertions.assertThrows(IllegalArgumentException.class, ()->  apiClient.createNamespace(null));
        Assertions.assertThrows(IllegalArgumentException.class, ()->  apiClient.createNamespace(""));
    }

    @Test
    public void shouldCreateNamespaceSimpleStrategy() {
        // Given
        DataApiClient apiClient = DataApiClients.create();
        DataApiNamespace ns2 = apiClient.createNamespace("ns2",
                CreateNamespaceOptions.simpleStrategy(1));
        Assertions.assertNotNull(ns2);
        Assertions.assertTrue(apiClient.isNamespaceExists("ns2"));
    }

    @Test
    public void shouldCreateNamespaceNetworkStrategy() {
        // Given
        DataApiClient apiClient = DataApiClients.create();
        // When
        DataApiNamespace ns3 = apiClient.createNamespace("ns3",
                CreateNamespaceOptions.networkTopologyStrategy(Map.of("datacenter1", 1)));
        Assertions.assertTrue(apiClient.isNamespaceExists("ns3"));
        // Then
        Assertions.assertNotNull(ns3);
        Assertions.assertTrue(apiClient.isNamespaceExists("ns3"));
        Assertions.assertThrows(DataApiException.class, () -> apiClient.createNamespace("ns4",
                CreateNamespaceOptions.networkTopologyStrategy(Map.of("invalid", 1))));

        // When
        Assertions.assertThrows(IllegalArgumentException.class, ()->  apiClient.dropNamespace(null));
        Assertions.assertThrows(IllegalArgumentException.class, ()->  apiClient.dropNamespace(""));
        apiClient.dropNamespace("ns3");
        Assertions.assertFalse(apiClient.isNamespaceExists("ns1"));
    }

    @Test
    public void shouldAccessNamespace() {
        // Given
        DataApiClient apiClient = DataApiClients.create();
        apiClient.createNamespace("ns2");
        Assertions.assertTrue(apiClient.listNamespaceNames().anyMatch("ns2"::equals));
        Assertions.assertTrue(apiClient.isNamespaceExists("ns2"));

        Assertions.assertThrows(NamespaceNotFoundException.class, () -> apiClient.getNamespace("invalid"));
        DataApiNamespace ns2 = apiClient.getNamespace("ns2");
        Assertions.assertNotNull(ns2);

        ns2.drop();
        Assertions.assertFalse(apiClient.isNamespaceExists("ns2"));
    }

}
