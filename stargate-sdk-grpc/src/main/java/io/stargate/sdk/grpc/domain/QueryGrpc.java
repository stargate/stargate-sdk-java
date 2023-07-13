package io.stargate.sdk.grpc.domain;

import io.stargate.proto.QueryOuterClass;

import java.util.*;

/**
 * Represent a query againt GRPC enpoind
 */
public class QueryGrpc {

    /** Default page size. */
    public  static final int DEFAULT_PAGE_SIZE = 5000;

    /** cql statement. */
    private final CqlStatementGrpc cqlStatement;

    /** keyspace. */
    private String keyspace;

    /** consistency level. */
    private QueryOuterClass.Consistency consistencyLevel = QueryOuterClass.Consistency.LOCAL_QUORUM;

    /** page size. */
    private int pageSize = DEFAULT_PAGE_SIZE;

    /** query paging state. */
    private String pagingState;

    /** time stamp. */
    private long timestamp = 0L;

    /** tracing. */
    private boolean tracing = false;

    /**
     * Default constructor.
     *
     * @param cql
     *      current cql query
     */
    public QueryGrpc(String cql) {
        this.cqlStatement = new CqlStatementGrpc(cql);
    }

    /**
     * Constructor with Params.
     *
     * @param cql
     *      current cql query
     * @param params
     *      query items
     */
    public QueryGrpc(String cql, Object... params) {
        this.cqlStatement = new CqlStatementGrpc(cql, params);
    }

    /**
     * Constructor with Params.
     *
     * @param cql
     *      current cql query
     * @param params
     *      query items
     */
    public QueryGrpc(String cql, Map<String, Object> params) {
        this.cqlStatement = new CqlStatementGrpc(cql);
    }

    /**
     * Builder setter.
     *
     * @param cl
     *      consistency level
     * @return
     *      current reference
     */
    public QueryGrpc setConsistencyLevel(QueryOuterClass.Consistency cl) {
        this.consistencyLevel = cl;
        return this;
    }

    /**
     * Builder setter.
     * @param keyspace
     *      keyspace
     * @return
     *      current reference
     */
    public QueryGrpc setKeyspace(String keyspace) {
        this.keyspace = keyspace;
        return this;
    }

    /**
     * Builder setter.
     * @param pageSize
     *      page size
     * @return
     *      current reference
     */
    public QueryGrpc setPageSize(int pageSize) {
        this.pageSize = pageSize;
        return this;
    }

    /**
     * Builder setter.
     * @param pagingState
     *      value for pagingState
     * @return
     *      current reference
     */
    public QueryGrpc setPagingState(String pagingState) {
        this.pagingState = pagingState;
        return this;
    }

    /**
     * Set value for timestamp.
     *
     * @param timestamp
     *      new value for timestamp
     * @return
     *      current reference
     */
    public QueryGrpc setTimestamp(long timestamp) {
        this.timestamp = timestamp;
        return this;
    }

    /**
     * update tracing.
     *
     * @param tracing
     *      flag tracing
     * @return
     *     current reference
     */
    public QueryGrpc setTracing(boolean tracing) {
        this.tracing = tracing;
        return this;
    }

    /**
     * Gets tracing
     *
     * @return value of tracing
     */
    public boolean isTracing() {
        return tracing;
    }

    /**
     * Gets cql
     *
     * @return value of cql
     */
    public CqlStatementGrpc getCqlStatement() {
        return cqlStatement;
    }

    /**
     * Gets keyspace
     *
     * @return value of keyspace
     */
    public String getKeyspace() {
        return keyspace;
    }

    /**
     * Gets consistencyLevel
     *
     * @return value of consistencyLevel
     */
    public QueryOuterClass.Consistency getConsistencyLevel() {
        return consistencyLevel;
    }

    /**
     * Gets pageSize
     *
     * @return value of pageSize
     */
    public int getPageSize() {
        return pageSize;
    }

    /**
     * Gets pagingState
     *
     * @return value of pagingState
     */
    public String getPagingState() {
        return pagingState;
    }

    /**
     * Gets timestamp
     *
     * @return value of timestamp
     */
    public long getTimestamp() {
        return timestamp;
    }
}
