package io.stargate.sdk.json.domain;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.sdk.utils.Assert;
import lombok.Getter;

import java.util.HashMap;
import java.util.Map;

/**
 * Filter Builder.
 */
@Getter
public class JsonFilter {

    static final ObjectMapper JACKSON_MAPPER = new ObjectMapper();

    @JsonInclude(JsonInclude.Include.NON_NULL)
    Map<String, Object> filter;

    /**
     * Default constructor.
     */
    public JsonFilter() {}

    /**
     * Default constructor.
     *
     * @param json
     *      filter expression as JSON
     */
    public JsonFilter(String json) {
        try {
            this.filter = JACKSON_MAPPER.readValue(json, Map.class);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Cannot parse json", e);
        }
    }

    /**
     * Work with arguments.
     *
     * @param fieldName
     *      current field name.
     * @return
     *      builder for the filter
     */
    public JsonFilterBuilder where(String fieldName) {
        Assert.hasLength(fieldName, "fieldName");
        if (filter != null) {
            throw new IllegalArgumentException("Invalid query please use and() " +
                    "as a where clause has been provided");
        }
        filter = new HashMap<>();
        return new JsonFilterBuilder(this, fieldName);
    }

    /**
     * Only return those fields if provided.
     *
     * @param fieldName
     *          field name
     * @return SearchDocumentWhere
     *          current builder
     */
    public JsonFilterBuilder andWhere(String fieldName) {
        Assert.hasLength(fieldName, "fieldName");
        if (filter == null || filter.isEmpty()) {
            throw new IllegalArgumentException("Invalid query please use where() " +
                    "as a where clause has been provided");
        }
        return new JsonFilterBuilder(this, fieldName);
    }


}
