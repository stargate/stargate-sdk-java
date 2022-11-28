package io.stargate.sdk.grpc.domain;

import java.util.*;

/**
 * Bean holding grpc batch properties.
 */
public class CqlStatementGrpc {

    /** query param 1. */
    private List<Object> positionalValues = new ArrayList<>();

    /** query param 2. */
    private Map<String, Object> namedValues = new HashMap<>();

    /** query. */
    private final String cql;

    /**
     * Default constructor.
     *
     * @param cql
     *      current cql query
     */
    public CqlStatementGrpc(String cql) {
        this.cql = cql;
    }

    /**
     * Constructor with Params.
     *
     * @param cql
     *      current cql query
     * @param params
     *      query items
     */
    public CqlStatementGrpc(String cql, Object... params) {
        this.cql = cql;
        if (params != null) {
            this.positionalValues = Arrays.asList(params);
        }
    }

    /**
     * Constructor with Params.
     *
     * @param cql
     *      current cql query
     * @param params
     *      query items
     */
    public CqlStatementGrpc(String cql, Map<String, Object> params) {
        this.cql = cql;
        if (params != null) {
            this.namedValues = params;
        }
    }

    /**
     * Builder setter.
     * @param positionalValues
     *      param values
     * @return
     *      current reference
     */
    public CqlStatementGrpc setPositionalValues(List<Object> positionalValues) {
        if (!this.namedValues.isEmpty()) {
            throw new IllegalArgumentException("You cannot have both positional and named parameters");
        }
        this.positionalValues = positionalValues;
        return this;
    }

    /**
     * Builder setter.
     *
     * @param positionalValue
     *      param values
     * @return
     *      current reference
     */
    public CqlStatementGrpc addPositionalValue(Object positionalValue) {
        if (!this.namedValues.isEmpty()) {
            throw new IllegalArgumentException("You cannot have both positional and named parameters");
        }
        this.positionalValues.add(positionalValue);
        return this;
    }

    /**
     * Builder setter.
     * @param namedValues
     *      param values
     * @return
     *      current reference
     */
    public CqlStatementGrpc setNamedValues(Map<String, Object> namedValues) {
        if (!this.positionalValues.isEmpty()) {
            throw new IllegalArgumentException("You cannot have both positional and named parameters");
        }
        this.namedValues = namedValues;
        return this;
    }

    /**
     * Builder setter.
     *
     * @param key
     *      new key
     * @param namedValue
     *      param values
     * @return
     *      current reference
     */
    public CqlStatementGrpc putNamedValue(String key, Object namedValue) {
        if (!this.positionalValues.isEmpty()) {
            throw new IllegalArgumentException("You cannot have both positional and named parameters");
        }
        this.namedValues.put(key, namedValue);
        return this;
    }

    /**
     * Gets positionalValues
     *
     * @return value of positionalValues
     */
    public List<Object> getPositionalValues() {
        return positionalValues;
    }

    /**
     * Gets namedValues
     *
     * @return value of namedValues
     */
    public Map<String, Object> getNamedValues() {
        return namedValues;
    }

    /**
     * Gets cql
     *
     * @return value of cql
     */
    public String getCql() {
        return cql;
    }
}
