package io.stargate.sdk.json.domain;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.sdk.utils.Assert;
import io.stargate.sdk.utils.JsonUtils;
import lombok.Getter;

import java.util.HashMap;
import java.util.Map;

/**
 * Filter Builder.
 */
@Getter
public class Filter {

    @JsonInclude(JsonInclude.Include.NON_NULL)
    Map<String, Object> filter;

    /**
     * Default constructor.
     */
    public Filter() {}

    /**
     * Default constructor.
     *
     * @param json
     *      filter expression as JSON
     */
    @SuppressWarnings("unchecked")
    public Filter(String json) {
        this.filter = JsonUtils.unmarshallBean(json, Map.class);
    }

    /**
     * Work with arguments.
     *
     * @param fieldName
     *      current field name.
     * @return
     *      builder for the filter
     */
    public FilterBuilder where(String fieldName) {
        Assert.hasLength(fieldName, "fieldName");
        if (filter != null) {
            throw new IllegalArgumentException("Invalid query please use and() " +
                    "as a where clause has been provided");
        }
        filter = new HashMap<>();
        return new FilterBuilder(this, fieldName);
    }

    /**
     * Only return those fields if provided.
     *
     * @param fieldName
     *          field name
     * @return SearchDocumentWhere
     *          current builder
     */
    public FilterBuilder andWhere(String fieldName) {
        Assert.hasLength(fieldName, "fieldName");
        if (filter == null || filter.isEmpty()) {
            throw new IllegalArgumentException("Invalid query please use where() " +
                    "as a where clause has been provided");
        }
        return new FilterBuilder(this, fieldName);
    }


}
