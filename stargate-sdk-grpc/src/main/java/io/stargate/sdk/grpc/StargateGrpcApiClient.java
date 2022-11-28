package io.stargate.sdk.grpc;

import com.evanlennick.retry4j.config.RetryConfig;
import io.stargate.proto.QueryOuterClass;
import io.stargate.sdk.ServiceDatacenter;
import io.stargate.sdk.ServiceDeployment;
import io.stargate.sdk.api.TokenProvider;
import io.stargate.sdk.audit.ServiceCallObserver;
import io.stargate.sdk.core.domain.Page;
import io.stargate.sdk.core.domain.Row;
import io.stargate.sdk.core.domain.RowMapper;
import io.stargate.sdk.core.domain.RowResultPage;
import io.stargate.sdk.grpc.domain.BatchGrpc;
import io.stargate.sdk.grpc.domain.QueryGrpc;
import io.stargate.sdk.grpc.domain.ResultSetGrpc;
import io.stargate.sdk.http.auth.TokenProviderHttpAuth;
import io.stargate.sdk.utils.AnsiUtils;
import io.stargate.sdk.utils.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Wrapper to interact with GRPC Client.
 *
 * @author Cedrick LUNVEN (@clunven)
 */
public class StargateGrpcApiClient {
    
    /** Logger for our Client. */
    private static final Logger LOGGER = LoggerFactory.getLogger(StargateGrpcApiClient.class);

    /** default endpoint. */
    private static final String DEFAULT_HOST = "localhost";

    /** default endpoint. */
    private static final int DEFAULT_PORT = 8090;

    /** default endpoint. */
    private static final String PATH_HEALTH_CHECK = "/stargate/health";

    /** default endpoint. */
    private static final int DEFAULT_HEALTH_CHECK_PORT = 8084;

    /** default service id. */
    private static final String DEFAULT_SERVICE_ID = "sgv2-grpc";

    /** default datacenter id. */
    private static final String DEFAULT_DATACENTER = "dc1";

    /** Stargate grpc Client. */
    private final GrpcClientLoadBalanced lbGrpcClient;

    /**
     * Default Constructor.
     */
    public StargateGrpcApiClient() {
        this(DEFAULT_HOST + ":" + DEFAULT_PORT,
        DEFAULT_HOST + ":" + DEFAULT_HEALTH_CHECK_PORT + PATH_HEALTH_CHECK);
    }

    /**
     * Constructor with StargateClient as argument.
     *
     * @param serviceDeployment
     *      stargate deployment
     */
    public StargateGrpcApiClient(ServiceDeployment<ServiceGrpc> serviceDeployment) {
        Assert.notNull(serviceDeployment, "stargate client reference. ");
        this.lbGrpcClient = new GrpcClientLoadBalanced(serviceDeployment);
        LOGGER.info("+ API Grpc     :[" + AnsiUtils.green("{}") + "]", "ENABLED");
    }

    /**
     * Single instance of Stargate, could be used for tests.
     *
     * @param endpoint
     *      service endpoint
     * @param healthCheckUrl
     *      service health check
     */
    public StargateGrpcApiClient(String endpoint, String healthCheckUrl) {
        Assert.hasLength(endpoint, "stargate grpc endpoint");
        Assert.hasLength(healthCheckUrl, "stargate grpc health check");
        // Single instance running
        ServiceGrpc rest = new ServiceGrpc(DEFAULT_SERVICE_ID, endpoint, healthCheckUrl);
        // Api provider
        TokenProvider tokenProvider = new TokenProviderHttpAuth();
        // DC with default auth and single node
        ServiceDatacenter<ServiceGrpc> sDc = new ServiceDatacenter<>(DEFAULT_DATACENTER, tokenProvider, Collections.singletonList(rest));
        // Deployment with a single dc
        ServiceDeployment<ServiceGrpc>  deploy = new ServiceDeployment<ServiceGrpc>().addDatacenter(sDc);
        this.lbGrpcClient  = new GrpcClientLoadBalanced(deploy);
    }

    /**
     * Execute a query. Work with a pageableQuery
     *
     * findAll()
     * executePage()
     *
     * @param query
     *      current query
     * @return
     *      value
     */
    public RowResultPage execute(QueryGrpc query) {
        return mapFromResultSet(lbGrpcClient.execute(query));
    }

    /**
     * Execute and map as a Page.
     *
     * @param query
     *      current query
     * @param mapper
     *      mapper to object
     * @param <T>
     *      current type
     * @return
     *      page of element
     */
    public <T> Page<T> execute(QueryGrpc query, RowMapper<T> mapper) {
       return mapFromRowResultPage(execute(query), mapper);
    }

    /**
     * Execute a request.
     *
     * @param cql
     *      cql query
     * @return
     *      list of value
     */
    public RowResultPage execute(String cql) {
        return execute(new QueryGrpc(cql));
    }


    public <T> Page<T> execute(String cql, RowMapper<T> mapper) {
        return mapFromRowResultPage(execute(new QueryGrpc(cql)), mapper);
    }

    public <T> Stream<T> findAll(String cql, RowMapper<T> mapper) {

        return null;
    }

    /*
    public Stream<Row> findAll(String cql) {
        return null;
    }

    private Stream<Row> findAll(QueryGrpc pageQuery, PageSupplier<Row> pageLoader) {
        List<Row> documents = new ArrayList<>();
        // Loop on pages up to no more pages (could be done)
        String pageState = null;
        do {
            Page<Row> pageX = execute(this, pageQuery);
            if (pageX.getPageState().isPresent())  {
                pageState = pageX.getPageState().get();
            } else {
                pageState = null;
            }
            documents.addAll(pageX.getResults());
            // Reuissing query for next page
            pageQuery.setPageState(pageState);
        } while(pageState != null);
        return documents.stream();
    }*/

    private RowResultPage mapFromResultSet(ResultSetGrpc rs) {
        List<Row> rows = new ArrayList<>();
        rs.getRows().forEach(rgrpc -> {
            Row rowMap = new Row();
            for(String col : rs.getColumnsNames()) {
                QueryOuterClass.Value v = rgrpc.getValue(col);
                if (v != null) rowMap.put(col, v);
            }
        });
        return new RowResultPage(rs.getRowCount(), rs.getPagingState().orElse(null), rows);
    }

    private <T> Page<T> mapFromRowResultPage(RowResultPage rrp, RowMapper<T> mapper) {
        return new Page<T>(
                rrp.getPageSize(),
                rrp.getPageState().orElse(null),
                rrp.getResults().stream()
                        .map(mapper::map)
                        .collect(Collectors.toList()));
    }

    /**
     * Execute a request externalizing items.
     *
     * @param cql
     *      cql query
     * @param params
     *      cql params
     * @return
     *      params
     */
    public RowResultPage execute(String cql, Object... params) {
        return execute(new QueryGrpc(cql, params));
    }

    /**
     * Execute a request externalizing items.
     *
     * @param cql
     *      cql query
     * @param params
     *      cql params
     * @return
     *      params
     */
    public RowResultPage execute(String cql, Map<String, Object > params) {
        return execute(new QueryGrpc(cql, params));
    }

    /**
     * Execute a CQL Query asynchronously.
     *
     * @param query
     *      current query
     * @return
     *      callback
     */
    public CompletableFuture<ResultSetGrpc> executeAsync(QueryGrpc query) {
        return lbGrpcClient.executeAsync(query);
    }

    /**
     * Execute a request.
     *
     * @param cql
     *      cql query
     * @return
     *      list of value
     */
    public CompletableFuture<ResultSetGrpc> executeAsync(String cql) {
        return executeAsync(new QueryGrpc(cql));
    }

    /**
     * Execute a request externalizing items.
     *
     * @param cql
     *      cql query
     * @param params
     *      cql params
     * @return
     *      params
     */
    public CompletableFuture<ResultSetGrpc> executeAsync(String cql, Object... params) {
        return executeAsync(new QueryGrpc(cql, params));
    }

    /**
     * Execute a request externalizing items.
     *
     * @param cql
     *      cql query
     * @param params
     *      cql params
     * @return
     *      params
     */
    public CompletableFuture<ResultSetGrpc> executeAsync(String cql, Map<String, Object > params) {
        return executeAsync(new QueryGrpc(cql, params));
    }

    /**
     * Execute a query getting back a flux
     * @param query
     *      input query
     * @return
     *      flux of data
     */
    public Mono<ResultSetGrpc> executeReactive(QueryGrpc query) {
        return lbGrpcClient.executeReactive(query);
    }

    /**
     * Execute a request.
     *
     * @param cql
     *      cql query
     * @return
     *      list of value
     */
    public Mono<ResultSetGrpc> executeReactive(String cql) {
        return executeReactive(new QueryGrpc(cql));
    }

    /**
     * Execute a request externalizing items.
     *
     * @param cql
     *      cql query
     * @param params
     *      cql params
     * @return
     *      params
     */
    public Mono<ResultSetGrpc> executeReactive(String cql, Object... params) {
        return executeReactive(new QueryGrpc(cql, params));
    }

    /**
     * Execute a request externalizing items.
     *
     * @param cql
     *      cql query
     * @param params
     *      cql params
     * @return
     *      params
     */
    public Mono<ResultSetGrpc> executeReactive(String cql, Map<String, Object > params) {
        return executeReactive(new QueryGrpc(cql, params));
    }

    /**
     * Execute a gRPC batch.
     *
     * @param grpcBatch
     *      batch query
     * @return
     *      responses
     */
    public ResultSetGrpc executeBatch(BatchGrpc grpcBatch) {
        return lbGrpcClient.executeBatch(grpcBatch);
    }

    /**
     * Return a page.
     *
     * @param query
     *      current query
     * @param clazz
     *      current class
     * @param <T>
     *      parameters
     * @return
     *      first page
     */
    public <T> Page<T> execute(QueryGrpc query, Class<T> clazz) {
        return null;
    }

    /**
     * Register a new listener.
     *
     * @param name
     *      current name
     * @param listener
     *      current listener
     */
    public static void registerListener(String name, ServiceCallObserver listener) {
        GrpcClient.getInstance().registerListener(name, listener);
    }

    /**
     * Override default retry Configuration.
     *
     * @param retryConfig new value for retryConfig
     */
    public void setupRetryConfig(RetryConfig retryConfig) {
        GrpcClient.getInstance().setRetryConfig(retryConfig);
    }

}
