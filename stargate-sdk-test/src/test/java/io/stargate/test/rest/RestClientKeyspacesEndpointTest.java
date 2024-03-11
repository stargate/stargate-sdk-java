package io.stargate.test.rest;

import io.stargate.sdk.ServiceDatacenter;
import io.stargate.sdk.ServiceDeployment;
import io.stargate.sdk.auth.TokenProvider;
import io.stargate.sdk.http.ServiceHttp;
import io.stargate.sdk.auth.StargateAuthenticationService;
import io.stargate.sdk.rest.KeyspaceClient;
import io.stargate.sdk.rest.StargateRestApiClient;
import io.stargate.sdk.test.rest.AbstractRestClientKeyspacesTest;
import org.junit.jupiter.api.BeforeAll;

import java.util.Collections;

/**
 * Implementations of test for Data keyspace.
 * 
 * @author Cedrick LUNVEN (@clunven)
 */
public class RestClientKeyspacesEndpointTest extends AbstractRestClientKeyspacesTest {

    @BeforeAll
    public static void initStargateRestApiClient() {

        // Initialization
        stargateRestApiClient = new StargateRestApiClient("http://localhost:8082");

        // Single Endpoint (dev)
        ServiceHttp rest = new ServiceHttp("rest1", "http://localhost:8082", "http://localhost:8082/stargate/health");
        // Default Authentication Endpoint (dev)
        TokenProvider tokenProvider = new StargateAuthenticationService("cassandra", "cassandra", "http://localhost:8081");
        // A Service Datacenter with a single Service
        ServiceDatacenter<ServiceHttp> sDc = new ServiceDatacenter<>("dc1", tokenProvider, Collections.singletonList(rest));
        // A service deployment with a single DC
        ServiceDeployment<ServiceHttp> deploy = new ServiceDeployment<ServiceHttp>().addDatacenter(sDc);
        // Initialization of the service
        StargateRestApiClient restClient1 = new StargateRestApiClient(deploy);

        // PreRequisites
        KeyspaceClient ksClientTest    = stargateRestApiClient.keyspace(TEST_KEYSPACE);
        if (ksClientTest.exist()) ksClientTest.delete();

        KeyspaceClient ksClientTestBis = stargateRestApiClient.keyspace(TEST_KEYSPACE_BIS);
        if (ksClientTestBis.exist()) ksClientTestBis.delete();
    }

}
