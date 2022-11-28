package io.stargate.sdk.grpc.audit;

import io.stargate.sdk.api.ApiConstants;
import io.stargate.sdk.audit.ServiceCallEvent;
import io.stargate.sdk.grpc.ServiceGrpc;
import io.stargate.sdk.grpc.domain.BatchGrpc;
import io.stargate.sdk.grpc.domain.QueryGrpc;

/**
 * Event triggered for Api Invocation with input/output tracing.
 *
 * @author Cedrick LUNVEN (@clunven)
 */
public class ServiceGrpcCallEvent extends ServiceCallEvent<ServiceGrpc> implements ApiConstants {

    /** grpc Query. */
    protected final QueryGrpc query;

    /** grpc Batch. */
    protected final BatchGrpc batch;

    /** Response GRPC. */
    protected boolean success = true;

    /**
     * Constructor with grpc request.
     *
     * @param service
     *      current grpc Service
     * @param query
     *      current grpc query
     */
    public ServiceGrpcCallEvent(ServiceGrpc service, QueryGrpc query) {
        super();
        this.timestamp = System.currentTimeMillis();
        this.service  = service;
        this.query = query;
        this.batch = null;
    }

    /**
     * Constructor with grpc request.
     *
     * @param service
     *      current grpc Service
     * @param batch
     *      current grpc query
     */
    public ServiceGrpcCallEvent(ServiceGrpc service, BatchGrpc batch) {
        super();
        this.timestamp = System.currentTimeMillis();
        this.service  = service;
        this.batch = batch;
        this.query = null;
    }

    /**
     * Gets query
     *
     * @return value of query
     */
    public QueryGrpc getQuery() {
        return query;
    }

    /**
     * Gets success
     *
     * @return value of success
     */
    public boolean isSuccess() {
        return success;
    }

    /**
     * Set value for success
     *
     * @param success new value for success
     */
    public void setSuccess(boolean success) {
        this.success = success;
    }

    /**
     * Gets batch
     *
     * @return value of batch
     */
    public BatchGrpc getBatch() {
        return batch;
    }

}
