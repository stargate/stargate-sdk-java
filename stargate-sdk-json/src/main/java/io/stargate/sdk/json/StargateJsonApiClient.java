package io.stargate.sdk.json;

import io.stargate.sdk.ServiceDatacenter;
import io.stargate.sdk.ServiceDeployment;
import io.stargate.sdk.api.TokenProvider;
import io.stargate.sdk.http.LoadBalancedHttpClient;
import io.stargate.sdk.http.ServiceHttp;
import io.stargate.sdk.http.auth.TokenProviderHttpAuth;
import io.stargate.sdk.json.domain.JsonApiResponse;
import io.stargate.sdk.json.domain.NamespaceDefinition;
import io.stargate.sdk.utils.Assert;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;

import static io.stargate.sdk.json.utils.JsonApiClientUtils.executeOperation;
import static io.stargate.sdk.utils.AnsiUtils.green;

/**
 * Client for the JSON Document API.
 */
@Getter
@Slf4j
public class StargateJsonApiClient {

    /** default endpoint. */
    public static final String DEFAULT_ENDPOINT = "http://localhost:8181";

    /** default endpoint. */
    public static final String PATH_HEALTH_CHECK = "/stargate/health";

    /** default service id. */
    public static final String DEFAULT_SERVICE_ID = "sgv2-json";

    /** default datacenter id. */
    private static final String DEFAULT_DATACENTER = "dc1";

    /** path for json api. */
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
        log.info("+ API JSON     :[" + green("{}") + "]", "ENABLED");
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
        return execute("findNamespaces", null)
                .getStatusKeyAsStream("namespaces");
    }

    /**
     * Create a Namespace providing a name.
     *
     * @param namespace
     *      current namespace.
     */
    public void createNamespace(String namespace) {
        this.createNamespace(NamespaceDefinition.builder().name(namespace).build());
    }

   /**
     * Create a Namespace providing a name.
     *
     * @param req
     *      current namespace.
     */
    public void createNamespace(NamespaceDefinition req) {
        execute("createNamespace", req);
        log.info("Namespace  '" + green("{}") + "' has been created", req.getName());
    }

    /**
     * Drop a namespace, no error if it does snot exists.
     *
     * @param namespace
     *      current namespace
     */
    public void dropNamespace(String namespace) {
        execute("dropNamespace", Map.of("name", namespace));
        log.info("Namespace  '" + green("{}") + "' has been deleted", namespace);
    }

    /**
     * Syntax sugar.
     * @param operation
     *      operation to run
     * @param payload
     *      payload returned
     */
    private JsonApiResponse execute(String operation, Object payload) {
        return executeOperation(stargateHttpClient, rootResource, operation, payload);
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
