package io.stargate.sdk.v1.data.domain.query;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import io.stargate.sdk.http.domain.FilterKeyword;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Json Api Query Payload Wrapper.
 */
@Getter @Setter
@JsonInclude(JsonInclude.Include.NON_NULL)
public class SelectQuery {

    /**
     * Max page size.
     */
    public static final int PAGING_SIZE_MAX = 20;

    /**
     * Select.
     */
    private Map<String, Object> projection;

    /**
     * where clause
     */
    private Map<String, Object> filter;

    /**
     * Order by.
     */
    private Map<String, Object> sort;

    /**
     * Options.
     */
    private Map<String, Object> options;


    /**
     * Default constructor.
     */
    public SelectQuery() {}

    /**
     * We need a builder to create a query.
     *
     * @return
     *      builder
     */
    public static SelectQueryBuilder builder() {
        return new SelectQueryBuilder();
    }

    /**
     * Build a SQL query with a filter (no projection).
     *
     * @param pFilter
     *      current filter
     */
    public SelectQuery(Filter pFilter) {
        if (pFilter != null) {
            filter = new HashMap<>();
            filter.putAll(pFilter.filter);
        }
    }

    /**
     * Build a query with a filter (no projection).
     *
     * @param pFilter
     *      current filter
     * @param vector
     *      semantic search
     */
    public SelectQuery(float[] vector, Filter pFilter) {
        if (vector != null) {
            sort = new HashMap<>();
            sort.put(FilterKeyword.VECTOR.getKeyword(), vector);
        }
        if (pFilter != null) {
            filter = new HashMap<>();
            filter.putAll(pFilter.filter);
        }
    }

    /**
     * Constructor from a builder.
     *
     * @param builder
     *      current builder
     */
    public SelectQuery(SelectQueryBuilder builder) {
        // select
        this.projection = builder.projection;
        // where
        this.filter = builder.filter;
        // order by
        this.sort = builder.sort;
        // limit and option
        this.options = builder.options;
    }

    /**
     * Build the find by id request
     *
     * @param id
     *      identifier
     * @return
     *      query
     */
    public static SelectQuery findById(@NonNull String id) {
        return SelectQuery.builder().filter(Filter.findById(id)).build();
    }

    /**
     * Build the find by vector request
     *
     * @param vector
     *      document vector
     * @return
     *      query
     */
    public static SelectQuery findByVector(float[] vector) {
        if (vector == null) throw new IllegalArgumentException("vector cannot be null");
        return SelectQuery.builder().orderByAnn(vector).build();
    }

    /**
     * Build the find by vector request
     *
     * @param filter
     *      document vector
     * @return
     *      query
     */
    public static SelectQuery findWithFilter(Filter filter) {
        if (filter == null) throw new IllegalArgumentException("filter cannot be null");
        return SelectQuery.builder().filter(filter).build();
    }

    /**
     * Look for pageSize.
     *
     * @return
     *      page size
     */
    @JsonIgnore
    public Optional<Integer> getLimit() {
        if (options != null && options.containsKey("limit")) {
            return Optional.ofNullable((Integer) options.get("limit"));
        }
        return Optional.empty();
    }

    /**
     * Update page state
     *
     * @param pageState
     *      new value for page state
     */
    @JsonIgnore
    public void setPageState(String pageState) {
        if (options == null) {
            options = new HashMap<>();
        }
        options.put("pagingState", pageState);
    }

}
