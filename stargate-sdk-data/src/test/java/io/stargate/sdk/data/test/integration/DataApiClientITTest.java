package io.stargate.sdk.data.test.integration;

import io.stargate.sdk.auth.StargateAuthenticationService;
import io.stargate.sdk.data.client.DataApiClient;
import io.stargate.sdk.data.client.DataApiClients;
import io.stargate.sdk.data.client.Database;
import io.stargate.sdk.data.client.exception.DataApiException;
import io.stargate.sdk.data.client.model.namespaces.CreateNamespaceOptions;
import io.stargate.sdk.data.client.model.namespaces.NamespaceInformation;
import io.stargate.sdk.data.client.observer.LoggerCommandObserver;
import io.stargate.sdk.data.test.TestConstants;
import io.stargate.sdk.http.HttpClientOptions;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

/**
 * Test connectivity to API
 */
@Slf4j
class DataApiClientITTest implements TestConstants {

    // --------------------
    // -- Initialization
    // --------------------

    @Test
    void shouldGetToken() {
        assertThat(new StargateAuthenticationService().getToken()).isNotEmpty();
    }

    @Test
    void shouldConnectWithDefault() {
        // When
        DataApiClient apiDataApiClient = DataApiClients.create();
        apiDataApiClient.registerListener("logger", new LoggerCommandObserver(DataApiClient.class));
        // Then
        assertThat(apiDataApiClient).isNotNull();
        assertThat(apiDataApiClient.listNamespaceNames().count()).isGreaterThan(0);
        apiDataApiClient
                .listNamespaceNamesAsync()
                .thenApply(Stream::count)
                .thenAccept(count -> assertThat(count).isEqualTo(0));
    }

    @Test
    void shouldConnectWithEndpointAndToken() {
        // When
        DataApiClient apiDataApiClient = DataApiClients.create(
                DataApiClients.DEFAULT_ENDPOINT,
                new StargateAuthenticationService().getToken(),
                HttpClientOptions.builder()
                        .userAgentCallerName("stargate-sdk-data")
                        .userAgentCallerVersion("2.0")
                        .build());
        // Then
        assertThat(apiDataApiClient).isNotNull();
        apiDataApiClient.registerListener("logger", new LoggerCommandObserver("listNameSpaces"));
        assertThat(apiDataApiClient.listNamespaceNames().count()).isGreaterThan(0);

        apiDataApiClient.listNamespacesAsync()
                .thenApply(s -> s.map(NamespaceInformation::getName))
                .thenApply(Stream::count)
                .thenAccept(count -> assertThat(count).isGreaterThan(0));
    }

    // --------------------
    // -- create Namespace
    // --------------------

    @Test
    void shouldCreateNamespaceDefault() {
        // Given
        DataApiClient apiDataApiClient = DataApiClients.create();
        apiDataApiClient.registerListener("logger", new LoggerCommandObserver(DataApiClient.class));
        // When
        Database ns1 = apiDataApiClient.createNamespace(NAMESPACE_NS1);
        // Then
        assertThat(ns1).isNotNull();
        assertThat(apiDataApiClient.isNamespaceExists(NAMESPACE_NS1)).isTrue();
        assertThat(ns1.getNamespaceName()).isEqualTo(NAMESPACE_NS1);

        // When
        apiDataApiClient.createNamespaceAsync("ns2").thenAccept(dan -> assertThat(dan).isNotNull());

        // Surface
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> apiDataApiClient.createNamespace(null))
                .withMessage("Parameter 'namespace' should be null nor empty");
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> apiDataApiClient.createNamespace(""))
                .withMessage("Parameter 'namespace' should be null nor empty");

    }

    @Test
    void shouldCreateNamespaceSimpleStrategy() {
        // Given
        DataApiClient apiDataApiClient = DataApiClients.create();
        apiDataApiClient.registerListener("logger", new LoggerCommandObserver(DataApiClient.class));
        Database ns2 = apiDataApiClient.createNamespace("ns2",
                CreateNamespaceOptions.simpleStrategy(1));
        assertThat(ns2).isNotNull();
        assertThat(apiDataApiClient.isNamespaceExists("ns2")).isTrue();
    }

    @Test
    void shouldCreateNamespaceNetworkStrategy() {
        // Given
        DataApiClient apiDataApiClient = DataApiClients.create();
        apiDataApiClient.registerListener("logger", new LoggerCommandObserver(DataApiClient.class));
        // When
        Database ns3 = apiDataApiClient.createNamespace("ns3",
                CreateNamespaceOptions.networkTopologyStrategy(Map.of("datacenter1", 1)));
        assertThat(ns3).isNotNull();
        assertThat(apiDataApiClient.isNamespaceExists("ns3")).isTrue();

        // non-passing case
        assertThatExceptionOfType(DataApiException.class).isThrownBy(() ->
            apiDataApiClient.createNamespace("ns4",
                        CreateNamespaceOptions.networkTopologyStrategy(Map.of("invalid", 1)))
        );

        // DROP NAMESPACES
        apiDataApiClient.dropNamespace("ns3");
        assertThat(apiDataApiClient.isNamespaceExists("ns3")).isFalse();
        apiDataApiClient.dropNamespaceAsync("ns3");

        // non-passing case
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> apiDataApiClient.dropNamespace(null))
                .withMessage("Parameter 'namespace' should be null nor empty");
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> apiDataApiClient.dropNamespace(""))
                .withMessage("Parameter 'namespace' should be null nor empty");
    }

    @Test
    void shouldAccessNamespace() {
        // Given
        DataApiClient apiDataApiClient = DataApiClients.create();
        apiDataApiClient.registerListener("logger", new LoggerCommandObserver(DataApiClient.class));

        apiDataApiClient.createNamespace("ns2");
        assertThat(apiDataApiClient.listNamespaceNames())
                .as("Check if 'ns2' is present in the namespace names")
                .anyMatch("ns2"::equals);
        assertThat(apiDataApiClient.isNamespaceExists("ns2")).isTrue();

        Database ns2 = apiDataApiClient.getNamespace("ns2");
        assertThat(ns2).isNotNull();

        ns2.drop();
        assertThat(apiDataApiClient.isNamespaceExists("ns2")).isFalse();
    }

    @Test
    void shouldDropNamespace() {
        // Given
        DataApiClient apiDataApiClient = DataApiClients.create();
        apiDataApiClient.registerListener("logger", new LoggerCommandObserver(DataApiClient.class));

        assertThat(apiDataApiClient).isNotNull();
        Database tmp = apiDataApiClient.createNamespace("tmp", CreateNamespaceOptions.simpleStrategy(1));
        assertThat(tmp).isNotNull();
        assertThat(apiDataApiClient.isNamespaceExists("tmp")).isTrue();
        // When
        apiDataApiClient.dropNamespace("tmp");
        assertThat(apiDataApiClient.isNamespaceExists("tmp")).isFalse();
    }

}
