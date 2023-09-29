package io.stargate.sdk.json.domain;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Getter;

import java.util.HashMap;
import java.util.Map;

/**
 * Sample Query for Vector.
 */
@Getter
public class Query {

    @JsonInclude(JsonInclude.Include.NON_NULL)
    private Map<String, Object> sort;

    @JsonInclude(JsonInclude.Include.NON_NULL)
    private Map<String, Object> projection;

    @JsonInclude(JsonInclude.Include.NON_NULL)
    private Map<String, Object> options;

    @JsonInclude(JsonInclude.Include.NON_NULL)
    private Map<String, Object> filter;

    /**
     * Default constructor.
     */
    public Query() {}

    /**
     * We need a builder to create a query.
     *
     * @return
     *      builder
     */
    public static QueryBuilder builder() {
        return new QueryBuilder();
    }

    public Query(QueryBuilder builder) {
        this.sort = builder.sort;
        this.filter = builder.filter;
        this.projection = builder.projection;
        this.options = builder.options;
    }

}
