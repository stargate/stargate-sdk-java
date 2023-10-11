package io.stargate.sdk.json.domain;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Getter;

import java.util.Map;

/**
 * Json Api Query Payload Wrapper.
 */
@Getter
@JsonInclude(JsonInclude.Include.NON_NULL)
public class UpdateQuery {

    private Map<String, Object> sort;

    private Map<String, Object> options;

    private Map<String, Object> filter;

    private Map<String, Object> update;

    private JsonDocument replacement;

    /**
     * Default constructor.
     */
    public UpdateQuery() {}

    /**
     * We need a builder to create a query.
     *
     * @return
     *      builder
     */
    public static UpdateQueryBuilder builder() {
        return new UpdateQueryBuilder();
    }

    public UpdateQuery(UpdateQueryBuilder builder) {
        // where
        this.filter = builder.filter;
        // set
        this.update = builder.update;
        // replacement (findOneAndReplace)
        this.replacement = builder.replacement;
        // order by
        this.sort = builder.sort;
        // limit and option
        this.options = builder.options;
    }

}
