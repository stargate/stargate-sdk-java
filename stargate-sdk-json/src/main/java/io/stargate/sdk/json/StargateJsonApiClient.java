package io.stargate.sdk.json;

import io.stargate.sdk.ServiceDatacenter;
import io.stargate.sdk.ServiceDeployment;
import io.stargate.sdk.api.TokenProvider;
import io.stargate.sdk.http.LoadBalancedHttpClient;
import io.stargate.sdk.http.ServiceHttp;
import io.stargate.sdk.http.auth.TokenProviderHttpAuth;
import io.stargate.sdk.http.domain.ApiResponseHttp;
import io.stargate.sdk.json.domain.CreateNamespaceRequest;
import io.stargate.sdk.json.exception.JsonApiException;
import io.stargate.sdk.json.utils.JsonApOperationUtils;
import io.stargate.sdk.utils.Assert;
import io.stargate.sdk.utils.JsonUtils;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;

import static io.stargate.sdk.utils.AnsiUtils.green;

/**
 * Client for the JSON Document API.
 */
@Getter
public class StargateJsonApiClient {

    /** Logger for our Client. */
    private static final Logger LOGGER = LoggerFactory.getLogger(StargateJsonApiClient.class);

    /** default endpoint. */
    private static final String DEFAULT_ENDPOINT = "http://localhost:8181";

    /** default endpoint. */
    private static final String PATH_HEALTH_CHECK = "/stargate/health";

    /** default service id. */
    private static final String DEFAULT_SERVICE_ID = "sgv2-json";

    /** default datacenter id. */
    private static final String DEFAULT_DATACENTER = "dc1";

    public static final String PATH_V1  = "/v1";

    /**
     * /v1
     */
    public static Function<ServiceHttp, String> rootResource = (node) -> node.getEndpoint() + PATH_V1;

    /** Get Topology of the nodes. */
    protected final LoadBalancedHttpClient stargateHttpClient;

    /**
     * Default Constructor
     */
    public StargateJsonApiClient() {
        this(DEFAULT_ENDPOINT);
    }

    /**
     * Single instance of Stargate, could be used for tests.
     *
     * @param endpoint
     *      service endpoint
     */
    public StargateJsonApiClient(String endpoint) {
        Assert.hasLength(endpoint, "stargate endpoint");
        // Single instance running
        ServiceHttp rest = new ServiceHttp(DEFAULT_SERVICE_ID, endpoint, endpoint + PATH_HEALTH_CHECK);
        // Api provider
        TokenProvider tokenProvider = new TokenProviderHttpAuth();
        // DC with default auth and single node
        ServiceDatacenter<ServiceHttp> sDc =
                new ServiceDatacenter<>(DEFAULT_DATACENTER, tokenProvider, Collections.singletonList(rest));
        // Deployment with a single dc
        ServiceDeployment<ServiceHttp> deploy =
                new ServiceDeployment<ServiceHttp>().addDatacenter(sDc);
        this.stargateHttpClient  = new LoadBalancedHttpClient(deploy);
    }

    /**
     * Initialized document API with a URL and a token.
     *
     * @param serviceDeployment
     *      http client topology aware
     */
    public StargateJsonApiClient(ServiceDeployment<ServiceHttp> serviceDeployment) {
        Assert.notNull(serviceDeployment, "service deployment topology");
        this.stargateHttpClient = new LoadBalancedHttpClient(serviceDeployment);
        LOGGER.info("+ API JSON     :[" + green("{}") + "]", "ENABLED");
    }

    // ------------------------------------------
    // ----      Namespace operations        ----
    // ------------------------------------------

    /**
     * Find Namespaces.
     *
     * @return
     *       a list of namespaces
     */
    @SuppressWarnings("unchecked")
    public Stream<String> findNamespaces() {
        ApiResponseHttp res = stargateHttpClient.POST(rootResource, "{\"findNamespaces\": {}}");
        Map<?,?> body = JsonUtils.unmarshallBean(res.getBody(), Map.class);
        if (body.containsKey("status")) {
            Map<?,?> status = (Map<?,?>) body.get("status");
            if (status.containsKey("namespaces")) {
                return ((ArrayList<String>) status.get("namespaces")).stream();
            }
        }
        return Stream.empty();
    }

    /**
     * Create a Namespace providing a name.
     *
     * @param namespace
     *      current namespace.
     */
    public void createNamespace(String namespace) {
        this.createNamespace(CreateNamespaceRequest.builder().name(namespace).build());
    }

/**
     * Create a Namespace providing a name.
     *
     * @param req
     *      current namespace.
     */
    public void createNamespace(CreateNamespaceRequest req) {
        String stringBody = JsonApOperationUtils.buildRequestBody("createNamespace", req);
        ApiResponseHttp res = stargateHttpClient.POST(rootResource, stringBody);
        Map<?,?> body = JsonUtils.unmarshallBean(res.getBody(), Map.class);
        JsonApOperationUtils.handleErrors(body);
        if (body.containsKey("status")) {
            Map<?,?> status = (Map<?,?>) body.get("status");
            if (status.containsKey("ok")) {
                LOGGER.info("+ Namespace   :[" + green("{}") + "] created", req.getName());
            }
        } else {
            throw new JsonApiException("Cannot create namespace: " + body);
        }
    }

    /**
     * Drop a namespace, no error if it does snot exists.
     *
     * @param namespace
     *      current namespace
     */
    public void dropNamespace(String namespace) {
        String stringBody = JsonApOperationUtils.buildRequestBody("dropNamespace",
                JsonUtils.marshall(Map.of("name", namespace)));
        ApiResponseHttp res = stargateHttpClient.POST(rootResource, stringBody);
        if (res.getCode() != 200) {
            throw new JsonApiException("Cannot drop namespace: " + res.getBody());
        }
    }

    // ---------------------------------
    // ----    Sub Resources        ----
    // ---------------------------------

    /**
     * Move the document API (namespace client)
     *
     * @param namespace String
     * @return NamespaceClient
     */
    public JsonNamespacesClient namespace(String namespace) {
        return new JsonNamespacesClient(stargateHttpClient, namespace);
    }

}
