package io.stargate.sdk.json.domain;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Getter;
import lombok.NonNull;

import java.util.Map;

/**
 * Json Api Query Payload Wrapper.
 */
@Getter
@JsonInclude(JsonInclude.Include.NON_NULL)
public class DeleteQuery {

    private Map<String, Object> sort;

    private Map<String, Object> filter;

    /**
     * Default constructor.
     */
    public DeleteQuery() {}

    /**
     * We need a builder to create a query.
     *
     * @return
     *      builder
     */
    public static DeleteQueryBuilder builder() {
        return new DeleteQueryBuilder();
    }

    public DeleteQuery(DeleteQueryBuilder builder) {
        // where
        this.filter = builder.filter;
        // set
        // order by
        this.sort = builder.sort;
    }

    /**
     * Common request avalaible as static function.
     *
     * @param id
     *      identifier
     * @return
     *      query
     */
    public static DeleteQuery deleteById(@NonNull String id) {
        return DeleteQuery.builder().where("_id").isEqualsTo(id).build();
    }

    public static DeleteQuery deleteByVector(@NonNull float[] embeddings) {
        return DeleteQuery.builder().orderByAnn(embeddings).build();
    }

}