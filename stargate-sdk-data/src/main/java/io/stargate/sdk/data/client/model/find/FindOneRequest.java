package io.stargate.sdk.data.client.model.find;

import com.fasterxml.jackson.annotation.JsonInclude;
import io.stargate.sdk.data.client.model.Filter;
import lombok.Data;

import java.util.Map;

import static io.stargate.sdk.data.client.model.Filters.eq;

/**
 * Json Api Query Payload Wrapper.
 */
@Data
@JsonInclude(JsonInclude.Include.NON_NULL)
public class FindOneRequest {

    /**
     * where clause
     */
    private Map<String, Object> filter;

    /**
     * Order by.
     */
    private Map<String, Object> sort;

    /**
     * Select.
     */
    private Map<String, Integer> projection;

    /**
     * Options.
     */
    private FindOneCommandOptions options;

    /**
     * Options of the FindOne command.
     */
    @Data
    public static class FindOneCommandOptions {
        Boolean includeSimilarity;
    }

    public FindOneRequest() {
    }

    /**
     * Constructor with filter and options.
     * @param filter
     *      filter for the request
     * @param findOneOptions
     *      options for the request
     */
    public FindOneRequest(Filter filter, FindOneOptions findOneOptions) {
        if (filter != null) {
            this.filter = filter.getFilter();
        }
        if (findOneOptions != null) {
            this.sort = findOneOptions.getSort();
            this.options = findOneOptions.getOptions();
            this.projection = findOneOptions.getProjection();
        }
    }


}
