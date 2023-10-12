package io.stargate.sdk.grpc.domain;

import io.stargate.proto.QueryOuterClass;

import java.util.ArrayList;
import java.util.List;

/**
 * Bean holding grpc batch properties.
 */
public class BatchGrpc {

    /** keyspace. */
    private String keyspace;

    /** time stamp. */
    private long timestamp = 0L;

    /** tracing. */
    private boolean tracing = false;

    /** list of queries. */
    private List<CqlStatementGrpc> queries = new ArrayList<>();

    /** consistency level. */
    private QueryOuterClass.Consistency consistencyLevel = QueryOuterClass.Consistency.LOCAL_QUORUM;

    /**
     * Default constructor.
     */
    public BatchGrpc() {
    }

    /**
     * Update keyspace.
     *
     * @param keyspace
     *      keyspace value
     * @return
     *      current reference
     */
    public BatchGrpc setKeyspace(String keyspace) {
        this.keyspace = keyspace;
        return this;
    }

    /**
     * Update consistencyLevel.
     *
     * @param consistencyLevel
     *      keyspace value
     * @return
     *      current reference
     */
    public BatchGrpc setConsistencyLevel(QueryOuterClass.Consistency consistencyLevel) {
        this.consistencyLevel = consistencyLevel;
        return this;
    }

    /**
     * Update keyspace.
     *
     * @param timestamp
     *      timestamp value
     * @return
     *      current reference
     */
    public BatchGrpc setTimestamp(long timestamp) {
        this.timestamp = timestamp;
        return this;
    }

    /**
     * Update keyspace.
     *
     * @param tracing
     *      tracing value
     * @return
     *      current reference
     */
    public BatchGrpc setTracing(boolean tracing) {
        this.tracing = tracing;
        return this;
    }

    /**
     * UPdate list of queries.
     *
     * @param queries
     *      queries
     * @return
     *      current reference
     */
    public BatchGrpc setQueries(List<CqlStatementGrpc> queries) {
        this.queries = queries;
        return this;
    }

    /**
     * Add new query.
     *
     * @param q
     *      new query
     * @return
     *      current reference
     */
    public BatchGrpc addQuery(CqlStatementGrpc q) {
        this.queries.add(q);
        return this;
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
     * Gets timestamp
     *
     * @return value of timestamp
     */
    public long getTimestamp() {
        return timestamp;
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
     * Gets queries
     *
     * @return value of queries
     */
    public List<CqlStatementGrpc> getQueries() {
        return queries;
    }
}
