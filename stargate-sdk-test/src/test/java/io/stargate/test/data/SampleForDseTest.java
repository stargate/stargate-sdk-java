package io.stargate.test.data;

import io.stargate.sdk.ServiceDatacenter;
import io.stargate.sdk.ServiceDeployment;
import io.stargate.sdk.api.TokenProvider;
import io.stargate.sdk.data.DataApiClient;
import io.stargate.sdk.http.ServiceHttp;
import io.stargate.sdk.http.auth.TokenProviderHttpAuth;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.Collections;

class SampleForDseTest {

    String username = "cassandra";
    String password = "cassandra";
    String stargateAuthenticationUrl = "http://localhost:8081";
    String stargateDataApiUrl = "http://localhost:8181";
    String stargateDc = "dc1";

    @Test
    @Disabled("You need a dse instance running")
    public void testDseConnection() {

        // First you need credentials
        TokenProvider tokenProvider =
                new TokenProviderHttpAuth(username, password, stargateAuthenticationUrl);

        // Single Stargate Node
        ServiceHttp rest =
                new ServiceHttp("single_node", stargateDataApiUrl, stargateDataApiUrl + "/stargate/health");

        // DC with auth and single node (this client will LB the load on nodes of same DC)
        ServiceDatacenter<ServiceHttp> singleDc =
                new ServiceDatacenter<>(stargateDc, tokenProvider, Collections.singletonList(rest));

        // Deployment with a single dc (this client will failover across DC)
        DataApiClient jsonApiClient =
                new DataApiClient(new ServiceDeployment<ServiceHttp>().addDatacenter(singleDc));

        // Use the client
        jsonApiClient.namespace("default_keyspace").createCollection("my_collection", 1536);

    }
}
