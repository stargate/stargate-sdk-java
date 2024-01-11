package io.stargate.sdk.data.domain.query;

import com.fasterxml.jackson.annotation.JsonInclude;
import io.stargate.sdk.data.domain.JsonDocument;
import lombok.Getter;

import java.util.Map;

/**
 * Json Api Query Payload Wrapper.
 */
@Getter
@JsonInclude(JsonInclude.Include.NON_NULL)
public class UpdateQuery {

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
     * Update.
     */
    private Map<String, Object> update;

    /**
     * Replacement.
     */
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

    /**
     * Constructor with builder.
     *
     * @param builder
     *      builder
     */
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
