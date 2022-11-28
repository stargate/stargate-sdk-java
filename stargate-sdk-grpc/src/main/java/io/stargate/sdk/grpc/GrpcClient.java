package io.stargate.sdk.grpc;

import com.evanlennick.retry4j.CallExecutorBuilder;
import com.evanlennick.retry4j.Status;
import com.evanlennick.retry4j.config.RetryConfig;
import com.evanlennick.retry4j.config.RetryConfigBuilder;
import com.google.protobuf.*;
import io.stargate.grpc.StargateBearerToken;
import io.stargate.proto.QueryOuterClass;
import io.stargate.proto.StargateGrpc;
import io.stargate.sdk.api.ApiConstants;
import io.stargate.sdk.audit.ServiceCallObserver;
import io.stargate.sdk.grpc.audit.ServiceGrpcCallEvent;
import io.stargate.sdk.grpc.domain.BatchGrpc;
import io.stargate.sdk.grpc.domain.QueryGrpc;
import io.stargate.sdk.grpc.domain.ResultSetGrpc;
import io.stargate.sdk.grpc.utils.FuturesUtils;
import io.stargate.sdk.grpc.utils.StreamObserverToReactivePublisher;
import io.stargate.sdk.utils.CompletableFutures;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * Wrapping the HttpClient and provide helpers
 * 
 * @author Cedrick LUNVEN (@clunven)
 */
public class GrpcClient implements ApiConstants {

    /** Logger for our Client. */
    private static final Logger LOGGER = LoggerFactory.getLogger(GrpcClient.class);

    // -------------------------------------------
    // ----------------- Singleton ---------------
    // -------------------------------------------

    /** Singleton pattern. */
    private static GrpcClient _instance = null;

    /**
     * Hide default constructor
     */
    private GrpcClient() {}
    
    /**
     * Singleton Pattern.
     * 
     * @return
     *      singleton for the class
     */
    public static synchronized GrpcClient getInstance() {
        if (_instance == null) {
            _instance = new GrpcClient();
        }
        return _instance;
    }

    // -------------------------------------------
    // ----------------- Retries   ---------------
    // -------------------------------------------

    /** Default settings in Request and Retry */
    private static final int DEFAULT_RETRY_COUNT       = 3;

    /** Default settings in Request and Retry */
    private static final Duration DEFAULT_RETRY_DELAY  = Duration.ofMillis(100);

    /** Default retry configuration. */
    protected RetryConfig retryConfig = new RetryConfigBuilder()
            .retryOnAnyException()
            .withDelayBetweenTries(DEFAULT_RETRY_DELAY)
            .withExponentialBackoff()
            .withMaxNumberOfTries(DEFAULT_RETRY_COUNT)
            .build();

    /**
     * Set value for retryConfig
     *
     * @param retryConfig new value for retryConfig
     */
    public void setRetryConfig(RetryConfig retryConfig) {
        this.retryConfig = retryConfig;
    }

    // -------------------------------------------
    // ---------- Execute with GRPC --------------
    // -------------------------------------------

    /**
     * Execute a request coming from elsewhere.
     * 
     * @param sGrpc
     *      gRPC service
     * @param  query
     *      gRPC query
     * @param token
     *      authentication token
     * @return
     *      service response
     */
    public ResultSetGrpc execute(ServiceGrpc sGrpc, QueryGrpc query, String token) {
        // Initializing the invocation event
        ServiceGrpcCallEvent event = new ServiceGrpcCallEvent(sGrpc, query);
        // Create Stub
        StargateGrpc.StargateBlockingStub syncStub = StargateGrpc.newBlockingStub(sGrpc.getChannel())
                .withCallCredentials(new StargateBearerToken(token))
                .withDeadlineAfter(5, TimeUnit.SECONDS);
        try {
            // Execute
            Status< QueryOuterClass.Response> status = executeWithRetries(syncStub, mapGrpcQuery(query));
            // Audit Mapping
            event.setTotalTries(status.getTotalTries());
            event.setLastException(status.getLastExceptionThatCausedRetry());
            event.setResponseElapsedTime(status.getTotalElapsedDuration().toMillis());
            event.setResponseTimestamp(status.getEndTime());
            // Response Mapping
            return new ResultSetGrpc(status.getResult().getResultSet());
        } catch (RuntimeException e) {
            event.setErrorClass(e.getClass().getName());
            event.setErrorMessage(e.getMessage());
            throw e;
        } finally {
            CompletableFuture.runAsync(()-> notifyAsync(listener->listener.onCall(event)));
        }
    }


    /**
     * Execute a batch coming from elsewhere.
     *
     * @param sGrpc
     *      gRPC service
     * @param  batch
     *      gRPC query
     * @param token
     *      authentication token
     * @return
     *      service response
     */
    public ResultSetGrpc executeBatch(ServiceGrpc sGrpc, BatchGrpc batch, String token) {
        // Initializing the invocation event
        ServiceGrpcCallEvent event = new ServiceGrpcCallEvent(sGrpc, batch);
        // Create Stub
        StargateGrpc.StargateBlockingStub syncStub = StargateGrpc.newBlockingStub(sGrpc.getChannel())
                .withCallCredentials(new StargateBearerToken(token))
                .withDeadlineAfter(5, TimeUnit.SECONDS);
        try {
            // Execute
            long top =- System.currentTimeMillis();
            QueryOuterClass.Response response = syncStub.executeBatch(mapGrpcBatch(batch));
            event.setResponseElapsedTime(System.currentTimeMillis() - top);
            event.setResponseTimestamp(event.getResponseElapsedTime());
            return new ResultSetGrpc(response.getResultSet());
        } catch (RuntimeException e) {
            event.setErrorClass(e.getClass().getName());
            event.setErrorMessage(e.getMessage());
            throw e;
        } finally {
            CompletableFuture.runAsync(()-> notifyAsync(listener->listener.onCall(event)));
        }
    }

    /**
     * Execute a request coming from elsewhere.
     *
     * @param sGrpc
     *      gRPC service
     * @param  query
     *      gRPC query
     * @param token
     *      authentication token
     * @return
     *      service response
     */
    public CompletableFuture<ResultSetGrpc> executeAsync(ServiceGrpc sGrpc, QueryGrpc query, String token) {
        ServiceGrpcCallEvent event = new ServiceGrpcCallEvent(sGrpc, query);
        StargateGrpc.StargateFutureStub futureStub = StargateGrpc.newFutureStub(sGrpc.getChannel())
                .withCallCredentials(new StargateBearerToken(token))
                .withDeadlineAfter(5, TimeUnit.SECONDS);
        long startTime = System.currentTimeMillis();
        try {
            return FuturesUtils
                    .asCompletableFuture(futureStub.executeQuery(mapGrpcQuery(query)))
                    .thenApply(res -> {
                        event.setResponseTime(System.currentTimeMillis() - startTime);
                        return new ResultSetGrpc(res.getResultSet());
                    });
        } catch (RuntimeException e) {
            event.setErrorClass(e.getClass().getName());
            event.setErrorMessage(e.getMessage());
            throw e;
        } finally {
            CompletableFuture.runAsync(()-> notifyAsync(listener->listener.onCall(event)));
        }
    }

    /**
     * Execute a request coming from elsewhere.
     *
     * @param sGrpc
     *      gRPC service
     * @param  batch
     *      gRPC batch
     * @param token
     *      authentication token
     * @return
     *      service response
     */
    public CompletableFuture<ResultSetGrpc> executeBatchAsync(ServiceGrpc sGrpc, BatchGrpc batch, String token) {
        ServiceGrpcCallEvent event = new ServiceGrpcCallEvent(sGrpc, batch);
        StargateGrpc.StargateFutureStub futureStub = StargateGrpc.newFutureStub(sGrpc.getChannel())
                .withCallCredentials(new StargateBearerToken(token))
                .withDeadlineAfter(5, TimeUnit.SECONDS);
        long startTime = System.currentTimeMillis();
        try {
            return FuturesUtils
                    .asCompletableFuture(futureStub.executeBatch(mapGrpcBatch(batch)))
                    .thenApply(res -> {
                        event.setResponseTime(System.currentTimeMillis() - startTime);
                        return new ResultSetGrpc(res.getResultSet());
                    });
        } catch (RuntimeException e) {
            event.setErrorClass(e.getClass().getName());
            event.setErrorMessage(e.getMessage());
            throw e;
        } finally {
            CompletableFuture.runAsync(()-> notifyAsync(listener->listener.onCall(event)));
        }
    }

    /**
     * Execute a reactive query.
     *
     * @param sGrpc
     *      gRPC service
     * @param  query
     *      gRPC query
     * @param token
     *      authentication token
     * @return
     *      service response
     */
    public Mono<ResultSetGrpc> executeReactive(ServiceGrpc sGrpc, QueryGrpc query, String token) {
        ServiceGrpcCallEvent event = new ServiceGrpcCallEvent(sGrpc, query);
        StargateGrpc.StargateStub reactiveStub = StargateGrpc.newStub(sGrpc.getChannel())
                .withCallCredentials(new StargateBearerToken(token))
                .withDeadlineAfter(5, TimeUnit.SECONDS);
        long startTime = System.currentTimeMillis();
        try {
            StreamObserverToReactivePublisher streamObserverPublisher = new StreamObserverToReactivePublisher<QueryOuterClass.Response>();
            Mono<QueryOuterClass.Response> mono = Mono.from(streamObserverPublisher);
            reactiveStub.executeQuery(mapGrpcQuery(query), streamObserverPublisher);
            return mono.map(res -> new ResultSetGrpc(res.getResultSet()));
        } catch (RuntimeException e) {
            event.setErrorClass(e.getClass().getName());
            event.setErrorMessage(e.getMessage());
            throw e;
        } finally {
            CompletableFuture.runAsync(()-> notifyAsync(listener->listener.onCall(event)));
        }
    }

    /**
     * Execute a reactive query.
     *
     * @param sGrpc
     *      gRPC service
     * @param  batch
     *      gRPC batch
     * @param token
     *      authentication token
     * @return
     *      service response
     */
    public Mono<ResultSetGrpc> executeBatchReactive(ServiceGrpc sGrpc, BatchGrpc batch, String token) {
        ServiceGrpcCallEvent event = new ServiceGrpcCallEvent(sGrpc, batch);
        StargateGrpc.StargateStub reactiveStub = StargateGrpc.newStub(sGrpc.getChannel())
                .withCallCredentials(new StargateBearerToken(token))
                .withDeadlineAfter(5, TimeUnit.SECONDS);
        long startTime = System.currentTimeMillis();
        try {
            StreamObserverToReactivePublisher streamObserverPublisher = new StreamObserverToReactivePublisher<QueryOuterClass.Response>();
            Mono<QueryOuterClass.Response> mono = Mono.from(streamObserverPublisher);
            reactiveStub.executeBatch(mapGrpcBatch(batch), streamObserverPublisher);
            return mono.map(res -> new ResultSetGrpc(res.getResultSet()));
        } catch (RuntimeException e) {
            event.setErrorClass(e.getClass().getName());
            event.setErrorMessage(e.getMessage());
            throw e;
        } finally {
            CompletableFuture.runAsync(()-> notifyAsync(listener->listener.onCall(event)));
        }
    }

    /**
     * Map grpc Input base on the Grpc Query bean.
     *
     * @param batchGrpc
     *      current grpc batch
     * @return
     *      input Request forged
     */
    private QueryOuterClass.Batch mapGrpcBatch(BatchGrpc batchGrpc) {
        io.stargate.proto.QueryOuterClass.Batch.Builder batchBuilder = QueryOuterClass.Batch.newBuilder();

        QueryOuterClass.BatchParameters.Builder bbb = QueryOuterClass.BatchParameters.newBuilder();
        // Parameters
        //bbb.setConsistency();
        //bbb.setKeyspace();
        //bbb.setTimestamp();
        //bbb.setTracing();
        //batchBuilder.setParameters();

        QueryOuterClass.BatchQuery.newBuilder()
                .setCql("req1")
                .setValues(QueryOuterClass.Values.newBuilder().build());

        //for (String cqlQuery : batchQuery.) {
        //    batchBuilder.addQueries(
        //            QueryOuterClass.BatchQuery.newBuilder().setCql(cqlQuery).
        //                    build());
        //}
        //return getGrpcConnection()
        //        .getSyncStub()
        //        .executeBatch(batchBuilder.build());
        return null;
    }

    /**
     * Map grpc Input base on the Grpc Query bean.
     *
     * @param queryGrpc
     *      current grpc query
     * @return
     *      input Request forged
     */
    private QueryOuterClass.Query mapGrpcQuery(QueryGrpc queryGrpc) {
        // Core Query
        QueryOuterClass.Query.Builder queryBuilder = QueryOuterClass.Query.newBuilder();
        queryBuilder.setCql(queryGrpc.getCqlStatement().getCql());

        // Positional Values
        if (!queryGrpc.getCqlStatement().getPositionalValues().isEmpty()) {
            QueryOuterClass.Values.Builder valuesBuilder = QueryOuterClass.Values.newBuilder();
            queryGrpc.getCqlStatement().getPositionalValues().forEach(p -> valuesBuilder.addValues(mapGrpcValue(p)));
            queryBuilder.setValues(valuesBuilder);
        }

        // Named Values
        if (!queryGrpc.getCqlStatement().getNamedValues().isEmpty()) {
            QueryOuterClass.Values.Builder valuesBuilder = QueryOuterClass.Values.newBuilder();
            queryGrpc.getCqlStatement().getNamedValues().forEach((key, value) -> {
                valuesBuilder.addValueNames(key);
                valuesBuilder.addValues(mapGrpcValue(value));
            });
            queryBuilder.setValues(valuesBuilder);
        }

        // MetaData
        QueryOuterClass.QueryParameters.Builder queryParamsBuilder = QueryOuterClass.QueryParameters.newBuilder();
        if (null != queryGrpc.getConsistencyLevel()) {
            queryParamsBuilder.setConsistency(QueryOuterClass.ConsistencyValue
                    .newBuilder()
                    .setValue(queryGrpc.getConsistencyLevel()).build());
        }
        if (queryGrpc.getPageSize() > 0) {
            queryParamsBuilder.setPageSize(Int32Value.newBuilder()
                    .setValue(queryGrpc.getPageSize()).build());
        }
        if (queryGrpc.getPagingState() != null) {
            queryParamsBuilder.setPagingState(BytesValue.newBuilder()
                    .setValue(ByteString.copyFromUtf8(queryGrpc.getPagingState())).build());
        }
        if (queryGrpc.getKeyspace() != null) {
            queryParamsBuilder.setKeyspace(StringValue.newBuilder()
                    .setValue(queryGrpc.getKeyspace()));
        }
        if (queryGrpc.getTimestamp() > 0) {
            queryParamsBuilder.setTimestamp(Int64Value.of(queryGrpc.getTimestamp()));
        }
        if (queryGrpc.isTracing()) {
            queryParamsBuilder.setTracing(true);
        }
        queryBuilder.setParameters(queryParamsBuilder);
        return queryBuilder.build();
    }

    /**
     * Populate Parameter.
     *
     * @param o
     *      current param
     * @return
     *      current query
     */
    private QueryOuterClass.Value mapGrpcValue(Object o) {
        QueryOuterClass.Value.Builder vb = QueryOuterClass.Value.newBuilder();
        if (o instanceof String) {
            vb.setString((String) o);
        } else if (o instanceof Boolean){
            vb.setBoolean((Boolean) o);
        } else if (o instanceof byte[]) {
            vb.setBytes(ByteString.copyFrom((byte[]) o));
        } else if (o instanceof BigDecimal) {
            //vb.setDecimal(QueryOuterClass.Decimal.newBuilder().setValue(""));
        }
        /*
        vb.setDate()
        vb.setDecimal();
        vb.setDouble();
        vb.setFloat();
        vb.setInt();
        vb.setTime();

       vb.setNull();
       vb.setUnset();
       vb.setUuid();
       vb.setVarint();
       vb.setInet();

        vb.setCollection();
        vb.setUdt();*/
        return vb.build();
    }

    /**
     * Implementing retries.
     *
     * @param stub
     *      blocking stub
     * @return
     *      the closeable response
     */
    @SuppressWarnings("unchecked")
    private Status<QueryOuterClass.Response> executeWithRetries(StargateGrpc.StargateBlockingStub stub, QueryOuterClass.Query grpcQuery) {
        return new CallExecutorBuilder<QueryOuterClass.Response>()
                .config(retryConfig)
                .onSuccessListener(s -> CompletableFuture.runAsync(()-> notifyAsync(listener->listener.onSuccess(s))))
                .onCompletionListener(s -> CompletableFuture.runAsync(()-> notifyAsync(listener->listener.onCompletion(s))))
                .onFailureListener(s -> {
                    LOGGER.error("Calls failed after {} retries", s.getTotalTries());
                    CompletableFuture.runAsync(()-> notifyAsync(listener->listener.onFailure(s)));
                })
                .afterFailedTryListener(s -> {
                    LOGGER.error("Failure on attempt {}/{} ", s.getTotalTries(), retryConfig.getMaxNumberOfTries());
                    LOGGER.error("Failed request {} on {}", grpcQuery.getCql() ,stub.getChannel().toString() );
                    LOGGER.error("+ Exception was ", s.getLastExceptionThatCausedRetry());
                    CompletableFuture.runAsync(()-> notifyAsync(listener->listener.onFailedTry(s)));
                })
                .build()
                .execute(() -> stub.executeQuery(grpcQuery));
    }

    // -------------------------------------------
    // ----------      AUDIT        --------------
    // -------------------------------------------

    /** Observers. */
    protected static Map<String, ServiceCallObserver> apiInvocationsObserversMap = new ConcurrentHashMap<>();

    /**
     * Register a new listener.
     *
     * @param name
     *      current name
     * @param listener
     *      current listener
     */
    public static void registerListener(String name, ServiceCallObserver listener) {
        apiInvocationsObserversMap.put(name, listener);
    }

    /**
     * Asynchronously send calls to listener for tracing.
     *
     * @param lambda
     *      operations to execute
     * @return
     *      void
     */
    private void notifyAsync(Consumer<ServiceCallObserver> lambda) {
        CompletableFutures.allDone(apiInvocationsObserversMap.values().stream()
                .map(l -> CompletableFuture.runAsync(() -> lambda.accept(l)))
                .collect(Collectors.toList()));
    }

}
