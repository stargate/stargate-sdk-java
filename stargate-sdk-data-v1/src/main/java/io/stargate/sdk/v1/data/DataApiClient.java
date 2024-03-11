package io.stargate.sdk.v1.data;

import io.stargate.sdk.ServiceDatacenter;
import io.stargate.sdk.ServiceDeployment;
import io.stargate.sdk.api.TokenProvider;
import io.stargate.sdk.http.LoadBalancedHttpClient;
import io.stargate.sdk.http.ServiceHttp;
import io.stargate.sdk.http.auth.TokenProviderHttpAuth;
import io.stargate.sdk.v1.data.domain.ApiResponse;
import io.stargate.sdk.v1.data.domain.NamespaceDefinition;
import io.stargate.sdk.v1.data.exception.DataApiNamespaceNotFoundException;
import io.stargate.sdk.utils.Assert;
import io.stargate.sdk.v1.data.utils.DataApiUtils;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.Collections;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;

import static io.stargate.sdk.utils.AnsiUtils.green;

/**
 * Client core for Data API (crud for namespaces).
 */
@Slf4j
@Getter
public class DataApiClient {

    /** default endpoint. */
    public static final String DEFAULT_ENDPOINT = "http://localhost:8181";

    /** default endpoint. */
    public static final String PATH_HEALTH_CHECK = "/stargate/health";

    /** default service id. */
    public static final String DEFAULT_SERVICE_ID = "sgv2-json";

    /** default datacenter id. */
    private static final String DEFAULT_DATACENTER = "dc1";

    /** path for json api. */
    public static final String DEFAULT_VERSION = "v1";

    /** Function to compute the root. */
    public final Function<ServiceHttp, String> rootResource;

    /** Get Topology of the nodes. */
    protected final LoadBalancedHttpClient stargateHttpClient;

    /** Version of the API. */
    protected final String version;

    /**
     * Default Constructor
     */
    public DataApiClient() {
        this(DEFAULT_ENDPOINT, DEFAULT_VERSION);
    }

    /**
     * Single instance of Stargate, could be used for tests.
     *
     * @param endpoint
     *      service endpoint
     */
    public DataApiClient(String endpoint) {
        this(endpoint, DEFAULT_VERSION);
    }

    /**
     * Single instance of Stargate, could be used for tests.
     *
     * @param endpoint
     *      service endpoint
     * @param version
     *      provide version number
     */
    public DataApiClient(String endpoint, String version) {
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
        this.version             = version;
        this.rootResource        = (node) -> node.getEndpoint() +  "/" + version;
    }

    /**
     * Initialized document API with a URL and a token.
     *
     * @param serviceDeployment
     *      http client topology aware
     */
    public DataApiClient(ServiceDeployment<ServiceHttp> serviceDeployment) {
        this(serviceDeployment, DEFAULT_VERSION);
    }

    /**
     * Initialized document API with a URL and a token.
     *
     * @param serviceDeployment
     *      http client topology aware
     * @param version
     *      customized version
     */
    public DataApiClient(ServiceDeployment<ServiceHttp> serviceDeployment, String version) {
        Assert.notNull(serviceDeployment, "service deployment topology");
        this.stargateHttpClient = new LoadBalancedHttpClient(serviceDeployment);
        this.version             = version;
        this.rootResource        = (node) -> node.getEndpoint() +  "/" + version;
    }

    // ------------------------------------------
    // ----      Namespace operations        ----
    // ------------------------------------------

    /**
     * Evaluate if a namespace exists.
     *
     * @param namespace
     *      namespace name.
     * @return
     *      if namespace exists
     */
    public boolean isNamespaceExists(String namespace) {
        return findAllNamespaces().anyMatch(namespace::equals);
    }

    /**
     * Find Namespaces.
     *
     * @return
     *       a list of namespaces
     */
    public Stream<String> findAllNamespaces() {
        return execute("findNamespaces", null)
                .getStatusKeyAsStringStream("namespaces");
    }

    /**
     * Create a Namespace providing a name.
     *
     * @param namespace
     *      current namespace.
     * @return
     *      client for namespace
     */
    public NamespaceClient createNamespace(String namespace) {
        this.createNamespace(NamespaceDefinition.builder().name(namespace).build());
        return new NamespaceClient(this, namespace);
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
     * Drop a namespace, no error if it does not exist.
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
    private ApiResponse execute(String operation, Object payload) {
        return DataApiUtils.executeOperation(stargateHttpClient, rootResource, operation, payload);
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
    public NamespaceClient namespace(String namespace) {
        if (findAllNamespaces().noneMatch(namespace::equals)) throw new DataApiNamespaceNotFoundException(namespace);
        return new NamespaceClient(this, namespace);
    }

}
