package io.stargate.sdk.json.domain;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Getter;
import lombok.NonNull;

import java.util.HashMap;
import java.util.Map;

/**
 * Json Api Query Payload Wrapper.
 */
@Getter
@JsonInclude(JsonInclude.Include.NON_NULL)
public class SelectQuery {

    public static final int DEFAULT_PAGE_SIZE = 21;

    public static final int PAGING_SIZE_MAX = 20;

    private Map<String, Object> sort;

    private Map<String, Object> projection;

    private Map<String, Object> options;

    private Map<String, Object> filter;

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
     * Common request avalaible as static function.
     *
     * @param id
     *      identifier
     * @return
     *      query
     */
    public static SelectQuery findById(@NonNull String id) {
        return SelectQuery.builder().where("_id").isEqualsTo(id).build();
    }

    public static SelectQuery findByVector(@NonNull float[] embeddings) {
        return SelectQuery.builder().selectVector()
                .selectSimilarity()
                .orderByAnn(embeddings).build();
    }

    /**
     * Look for pageSize.
     * @return
     *      page size
     */
    @JsonIgnore
    public int getPageSize() {
        int pageSize = SelectQuery.DEFAULT_PAGE_SIZE;
        if (options != null && options.containsKey("limit")) {
            pageSize = (int) options.get("limit");
        }
        return pageSize;
    }

    @JsonIgnore
    public void setPageState(String pageState) {
        if (options == null) {
            options = new HashMap<>();
        }
        options.put("pagingState", pageState);
    }

}
