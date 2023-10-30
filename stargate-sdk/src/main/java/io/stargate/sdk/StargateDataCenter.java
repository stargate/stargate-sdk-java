package io.stargate.sdk;

import io.stargate.sdk.api.TokenProvider;
import io.stargate.sdk.grpc.ServiceGrpc;
import io.stargate.sdk.http.ServiceHttp;
import io.stargate.sdk.json.ApiClient;
import io.stargate.sdk.rest.StargateRestApiClient;

import java.util.ArrayList;
import java.util.List;

/**
 * CqlSession and Endpoints are associated to a dedicated DataCenter. The fail-over
 * will be performed by the SDK. As such a StargateClient will have multiple DC.
 *
 * @author Cedrick LUNVEN (@clunven)
 */
public class StargateDataCenter {

    /** datacenter unique id. */
    private String id;

    /** Multiple Nodes of Stargate share their token in a Cassandra table to can be positioned as DC level. */
    private TokenProvider tokenProvider;

    /** Inside a single datacenter I will have multiple Stargate Nodes. We will load-balance our queries among those instances. */
    private List<ServiceHttp> restNodes = new ArrayList<>();

    private List<ServiceHttp> docNodes = new ArrayList<>();

    private List<ServiceGrpc> grpcNodes = new ArrayList<>();

    private List<ServiceHttp> graphqlNodes = new ArrayList<>();

    private List<ServiceHttp> jsonApiNodes = new ArrayList<>();

    /**
     * Full constructor.
     *
     * @param id
     *      current dc name
     */
    public StargateDataCenter(String id) {
        this.id  = id;
    }

    /**
     * Add default Rest service
     *
     * @return
     *      data center
     */
    public StargateDataCenter withRest() {
        return addRestService(new ServiceHttp(
                StargateRestApiClient.DEFAULT_SERVICE_ID,
                StargateRestApiClient.DEFAULT_ENDPOINT,
                StargateRestApiClient.DEFAULT_ENDPOINT + StargateRestApiClient.PATH_HEALTH_CHECK)
        );
    }

    /**
     * Add default Rest service
     *
     * @return
     *      data center
     */
    public StargateDataCenter withJson() {
        return addJsonService(new ServiceHttp(
                ApiClient.DEFAULT_SERVICE_ID,
                ApiClient.DEFAULT_ENDPOINT,
                ApiClient.DEFAULT_ENDPOINT + ApiClient.PATH_HEALTH_CHECK)
        );
    }

    /**
     * Add a new node to the DC.
     *
     * @param s
     *      current node
     * @return
     *      current dc
     */
    public StargateDataCenter addRestService(ServiceHttp s) {
        restNodes.add(s);
        return this;
    }

    /**
     * Add a new node to the DC.
     *
     * @param s
     *      current node
     * @return
     *      current dc
     */
    public StargateDataCenter addGraphQLService(ServiceHttp s) {
        graphqlNodes.add(s);
        return this;
    }

    /**
     * Add a new node to the DC.
     *
     * @param s
     *      current node
     * @return
     *      current dc
     */
    public StargateDataCenter addDocumenService(ServiceHttp s) {
        docNodes.add(s);
        return this;
    }

    /**
     * Add a new node to the DC.
     *
     * @param s
     *      current node
     * @return
     *      current dc
     */
    public StargateDataCenter addJsonService(ServiceHttp s) {
        jsonApiNodes.add(s);
        return this;
    }

    /**
     * Add a new node to the DC.
     *
     * @param s
     *      current node
     * @return
     *      current dc
     */
    public StargateDataCenter addGrpcService(ServiceGrpc s) {
        grpcNodes.add(s);
        return this;
    }

    /**
     * Full constructor.
     *
     * @param id
     *      current dc id
     * @param tokenProvider
     *      token provider for the DC
     */
    public StargateDataCenter(String id, TokenProvider tokenProvider) {
        this(id);
        this.tokenProvider   = tokenProvider;
    }

    /**
     * Gets tokenProvider
     *
     * @return value of tokenProvider
     */
    public TokenProvider getTokenProvider() {
        return tokenProvider;
    }

    /**
     * Gets id
     *
     * @return value of id
     */
    public String getId() {
        return id;
    }

    /**
     * Set value for tokenProvider
     *
     * @param tokenProvider new value for tokenProvider
     */
    public void setTokenProvider(TokenProvider tokenProvider) {
        this.tokenProvider = tokenProvider;
    }

    /**
     * Gets restNodes
     *
     * @return value of restNodes
     */
    public List<ServiceHttp> getRestNodes() {
        return restNodes;
    }

    /**
     * Gets docNodes
     *
     * @return value of docNodes
     */
    public List<ServiceHttp> getDocNodes() {
        return docNodes;
    }

    /**
     * Gets grpcNodes
     *
     * @return value of grpcNodes
     */
    public List<ServiceGrpc> getGrpcNodes() {
        return grpcNodes;
    }

    /**
     * Gets json Api Nodes
     *
     * @return value of grpcNodes
     */
    public List<ServiceHttp> getJsonNodes() {
        return jsonApiNodes;
    }

    /**
     * Gets GraohQL Nodes
     *
     * @return value of grpcNodes
     */
    public List<ServiceHttp> getGraphqlNodes() {
        return graphqlNodes;
    }
}

