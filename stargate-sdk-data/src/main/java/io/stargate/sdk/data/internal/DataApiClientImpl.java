package io.stargate.sdk.data.internal;

import io.stargate.sdk.data.client.DataApiClient;
import io.stargate.sdk.data.client.DataApiNamespace;
import io.stargate.sdk.data.client.exception.NamespaceNotFoundException;
import io.stargate.sdk.data.client.model.CreateNamespaceOptions;
import io.stargate.sdk.data.internal.model.ApiResponse;
import io.stargate.sdk.data.internal.model.NamespaceInformation;
import io.stargate.sdk.ServiceDeployment;
import io.stargate.sdk.http.HttpClientOptions;
import io.stargate.sdk.http.LoadBalancedHttpClient;
import io.stargate.sdk.http.ServiceHttp;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;

import static io.stargate.sdk.utils.AnsiUtils.green;
import static io.stargate.sdk.utils.Assert.hasLength;
import static io.stargate.sdk.utils.Assert.notNull;

/**
 * Implementation of Client.
 */
@Slf4j
@Getter
public class DataApiClientImpl implements DataApiClient {

    /** Function to compute the root. */
    public final Function<ServiceHttp, String> rootResource;

    /** Get Topology of the nodes. */
    protected final LoadBalancedHttpClient stargateHttpClient;

    /** Version of the API. */
    protected final HttpClientOptions options;

    /**
     * Initialized document API with a URL and a token.
     *
     * @param serviceDeployment
     *      http client topology aware
     * @param httpClientOptions
     *      option for the client
     */
    public DataApiClientImpl(ServiceDeployment<ServiceHttp> serviceDeployment, HttpClientOptions httpClientOptions) {
        notNull(serviceDeployment, "service deployment topology");
        this.stargateHttpClient = new LoadBalancedHttpClient(serviceDeployment, httpClientOptions);
        this.options             = httpClientOptions;
        this.rootResource        = (node) -> node.getEndpoint() +  "/" + httpClientOptions.getApiVersion();
    }

    // ------------------------------------------
    // ----           Lookup                 ----
    // ------------------------------------------

    /** {@inheritDoc} */
    @Override
    public Function<ServiceHttp, String> lookup() {
        return rootResource;
    }

    /** {@inheritDoc} */
    @Override
    public LoadBalancedHttpClient getHttpClient() {
        return stargateHttpClient;
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
    public Stream<NamespaceInformation> listNamespaces() {
        return listNamespaceNames().map(NamespaceInformation::new);
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
        NamespaceInformation request = new NamespaceInformation();
        request.setName(namespace);
        request.setOptions(options);
        execute("createNamespace", request);
        log.info("Namespace  '" + green("{}") + "' has been created", namespace);
        return new DataApiNamespaceImpl(this, namespace);
    }



    /** {@inheritDoc} */
    public void dropNamespace(String namespace) {
        hasLength(namespace, "namespace");
        execute("dropNamespace", Map.of("name", namespace));
        log.info("Namespace  '" + green("{}") + "' has been deleted", namespace);
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
        return DataApiUtils.runCommand(stargateHttpClient, rootResource, operation, payload);
    }

}
