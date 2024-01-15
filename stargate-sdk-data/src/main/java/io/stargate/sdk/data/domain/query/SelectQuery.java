package io.stargate.sdk.data.domain.query;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
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
        return SelectQuery.builder().where("_id").isEqualsTo(id).build();
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