package io.stargate.sdk.data.test.integration;

import io.stargate.sdk.auth.StargateAuthenticationService;
import io.stargate.sdk.data.client.DataApiClient;
import io.stargate.sdk.data.client.DataApiClients;
import io.stargate.sdk.data.client.DataApiNamespace;
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
        DataApiClient apiClient = DataApiClients.create();
        apiClient.registerListener("logger", new LoggerCommandObserver(DataApiClient.class));
        // Then
        assertThat(apiClient).isNotNull();
        assertThat(apiClient.listNamespaceNames().count()).isGreaterThan(0);
        apiClient
                .listNamespaceNamesAsync()
                .thenApply(Stream::count)
                .thenAccept(count -> assertThat(count).isEqualTo(0));
    }

    @Test
    void shouldConnectWithEndpointAndToken() {
        // When
        DataApiClient apiClient = DataApiClients.create(
                DataApiClients.DEFAULT_ENDPOINT,
                new StargateAuthenticationService().getToken(),
                HttpClientOptions.builder()
                        .userAgentCallerName("stargate-sdk-data")
                        .userAgentCallerVersion("2.0")
                        .build());
        // Then
        assertThat(apiClient).isNotNull();
        apiClient.registerListener("logger", new LoggerCommandObserver("listNameSpaces"));
        assertThat(apiClient.listNamespaceNames().count()).isGreaterThan(0);

        apiClient.listNamespacesAsync()
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
        DataApiClient apiClient = DataApiClients.create();
        apiClient.registerListener("logger", new LoggerCommandObserver(DataApiClient.class));
        // When
        DataApiNamespace ns1 = apiClient.createNamespace(NAMESPACE_NS1);
        // Then
        assertThat(ns1).isNotNull();
        assertThat(apiClient.isNamespaceExists(NAMESPACE_NS1)).isTrue();
        assertThat(ns1.getName()).isEqualTo(NAMESPACE_NS1);

        // When
        apiClient.createNamespaceAsync("ns2").thenAccept(dan -> assertThat(dan).isNotNull());

        // Surface
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> apiClient.createNamespace(null))
                .withMessage("Parameter 'namespace' should be null nor empty");
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> apiClient.createNamespace(""))
                .withMessage("Parameter 'namespace' should be null nor empty");

    }

    @Test
    void shouldCreateNamespaceSimpleStrategy() {
        // Given
        DataApiClient apiClient = DataApiClients.create();
        apiClient.registerListener("logger", new LoggerCommandObserver(DataApiClient.class));
        DataApiNamespace ns2 = apiClient.createNamespace("ns2",
                CreateNamespaceOptions.simpleStrategy(1));
        assertThat(ns2).isNotNull();
        assertThat(apiClient.isNamespaceExists("ns2")).isTrue();
    }

    @Test
    void shouldCreateNamespaceNetworkStrategy() {
        // Given
        DataApiClient apiClient = DataApiClients.create();
        apiClient.registerListener("logger", new LoggerCommandObserver(DataApiClient.class));
        // When
        DataApiNamespace ns3 = apiClient.createNamespace("ns3",
                CreateNamespaceOptions.networkTopologyStrategy(Map.of("datacenter1", 1)));
        assertThat(ns3).isNotNull();
        assertThat(apiClient.isNamespaceExists("ns3")).isTrue();

        // non-passing case
        assertThatExceptionOfType(DataApiException.class).isThrownBy(() ->
            apiClient.createNamespace("ns4",
                        CreateNamespaceOptions.networkTopologyStrategy(Map.of("invalid", 1)))
        );

        // DROP NAMESPACES
        apiClient.dropNamespace("ns3");
        assertThat(apiClient.isNamespaceExists("ns3")).isFalse();
        apiClient.dropNamespaceAsync("ns3");

        // non-passing case
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> apiClient.dropNamespace(null))
                .withMessage("Parameter 'namespace' should be null nor empty");
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> apiClient.dropNamespace(""))
                .withMessage("Parameter 'namespace' should be null nor empty");
    }

    @Test
    void shouldAccessNamespace() {
        // Given
        DataApiClient apiClient = DataApiClients.create();
        apiClient.registerListener("logger", new LoggerCommandObserver(DataApiClient.class));

        apiClient.createNamespace("ns2");
        assertThat(apiClient.listNamespaceNames())
                .as("Check if 'ns2' is present in the namespace names")
                .anyMatch("ns2"::equals);
        assertThat(apiClient.isNamespaceExists("ns2")).isTrue();

        DataApiNamespace ns2 = apiClient.getNamespace("ns2");
        assertThat(ns2).isNotNull();

        ns2.drop();
        assertThat(apiClient.isNamespaceExists("ns2")).isFalse();
    }

    @Test
    void shouldDropNamespace() {
        // Given
        DataApiClient apiClient = DataApiClients.create();
        apiClient.registerListener("logger", new LoggerCommandObserver(DataApiClient.class));

        assertThat(apiClient).isNotNull();
        DataApiNamespace tmp = apiClient.createNamespace("tmp", CreateNamespaceOptions.simpleStrategy(1));
        assertThat(tmp).isNotNull();
        assertThat(apiClient.isNamespaceExists("tmp")).isTrue();
        // When
        apiClient.dropNamespace("tmp");
        assertThat(apiClient.isNamespaceExists("tmp")).isFalse();
    }

}
