package io.stargate.data_api.internal;

import io.stargate.data_api.client.DataApiClient;
import io.stargate.data_api.client.DataApiNamespace;
import io.stargate.data_api.client.exception.NamespaceNotFoundException;
import io.stargate.data_api.client.model.CreateNamespaceOptions;
import io.stargate.data_api.internal.model.CreateNamespaceRequest;
import io.stargate.data_api.internal.model.ApiResponse;
import io.stargate.sdk.ServiceDatacenter;
import io.stargate.sdk.ServiceDeployment;
import io.stargate.sdk.api.TokenProvider;
import io.stargate.sdk.http.LoadBalancedHttpClient;
import io.stargate.sdk.http.ServiceHttp;
import io.stargate.sdk.utils.Assert;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;

import static io.stargate.sdk.utils.AnsiUtils.green;
import static io.stargate.sdk.utils.AnsiUtils.yellow;
import static io.stargate.sdk.utils.Assert.hasLength;
import static io.stargate.sdk.utils.Assert.notNull;

/**
 * Implementation of Client.
 */
@Slf4j
@Getter
public class DataApiClientImpl implements DataApiClient {

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
     * Single instance of Stargate, could be used for tests.
     *
     * @param endpoint
     *      service endpoint
     * @param version
     *      provide version number
     */
    public DataApiClientImpl(String endpoint, String version, TokenProvider tokenProvider) {
        hasLength(endpoint, "stargate endpoint");
        notNull(tokenProvider, "tokenProvider");
        // Single instance running
        ServiceHttp rest = new ServiceHttp(DEFAULT_SERVICE_ID, endpoint, endpoint + PATH_HEALTH_CHECK);
        // DC with default auth and single node
        ServiceDatacenter<ServiceHttp> sDc =
                new ServiceDatacenter<>(DEFAULT_DATACENTER, tokenProvider, Collections.singletonList(rest));
        // Deployment with a single dc
        ServiceDeployment<ServiceHttp> deploy =
                new ServiceDeployment<ServiceHttp>().addDatacenter(sDc);
        this.stargateHttpClient  = new LoadBalancedHttpClient(deploy);
        this.version             = version;
        this.rootResource        = (node) -> node.getEndpoint() +  "/" + version;
        log.debug("Client initialized for endpoint [" + yellow(rootResource.apply(rest)) + "]");
    }

    /**
     * Initialized document API with a URL and a token.
     *
     * @param serviceDeployment
     *      http client topology aware
     */
    public DataApiClientImpl(ServiceDeployment<ServiceHttp> serviceDeployment) {
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
    public DataApiClientImpl(ServiceDeployment<ServiceHttp> serviceDeployment, String version) {
        notNull(serviceDeployment, "service deployment topology");
        this.stargateHttpClient = new LoadBalancedHttpClient(serviceDeployment);
        this.version             = version;
        this.rootResource        = (node) -> node.getEndpoint() +  "/" + version;
    }

    // ------------------------------------------
    // ----      Namespace operations        ----
    // ------------------------------------------

    /** {@inheritDoc} */
    @Override
    public Stream<String> listNamespaceNames() {
        return execute("findNamespaces", null).getStatusKeyAsStringStream("namespaces");
    }

    /** {@inheritDoc} */
    @Override
    public DataApiNamespace getNamespace(String namespaceName) {
        if (listNamespaceNames().noneMatch(namespaceName::equals)) throw new NamespaceNotFoundException(namespaceName);
        return new DataApiNamespaceImpl(this, namespaceName);
    }

    /** {@inheritDoc} */
    @Override
    public DataApiNamespace createNamespace(String namespace, CreateNamespaceOptions options) {
        hasLength(namespace, "namespace");
        CreateNamespaceRequest request = new CreateNamespaceRequest();
        request.setName(namespace);
        request.setOptions(options);
        execute("createNamespace", request);
        log.info("Namespace  '" + green("{}") + "' has been created", namespace);
        return new DataApiNamespaceImpl(this, namespace);
    }

    /** {@inheritDoc} */
    public void dropNamespace(String namespace) {
        hasLength("namespace", namespace);
        execute("dropNamespace", Map.of("name", namespace));
        log.info("Namespace  '" + green("{}") + "' has been deleted", namespace);
    }

    /** {@inheritDoc} */
    @Override
    public void close() throws IOException {
        // Close eventual listeners
    }

    /**
     * Syntax sugar.
     *
     * @param operation
     *      operation to run
     * @param payload
     *      payload returned
     */
    private ApiResponse execute(String operation, Object payload) {
        return DataApiUtils.executeOperation(stargateHttpClient, rootResource, operation, payload);
    }


}
