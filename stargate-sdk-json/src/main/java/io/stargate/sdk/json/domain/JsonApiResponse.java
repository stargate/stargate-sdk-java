package io.stargate.sdk.json.domain;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Data;
import lombok.NonNull;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

@Data
public class JsonApiResponse {

    /**
     * Return by everything except find*()
     */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private Map<String, Object> status;

    /**
     * If an error ocured
     */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private List<JsonApiError> errors;

    /**
     * Data retrieved by find*()
     */
    private JsonApiData data;

    /**
     * Syntax sugar.
     *
     * @param key
     *      key to be retrieved
     * @return
     *      list of values
     */
    @SuppressWarnings("unchecked")
    public Stream<String> getStatusKeyAsStream(@NonNull String key) {
        if (status.containsKey(key)) {
            return ((ArrayList<String>) status.get(key)).stream();
        }
        return Stream.empty();
    }


    /**
     * Syntax sugar.
     *
     * @param key
     *      key to be retrieved
     * @return
     *      list of values
     */
    public Integer getStatusKeyAsInt(@NonNull String key) {
        if (status.containsKey(key)) {
            return (Integer) status.get(key);
        }
        throw new IllegalArgumentException("Key '" + key + "' does not exist in status");
    }

}
