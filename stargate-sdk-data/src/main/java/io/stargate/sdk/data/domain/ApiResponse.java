package io.stargate.sdk.data.domain;

import com.fasterxml.jackson.annotation.JsonInclude;
import io.stargate.sdk.utils.JsonUtils;
import lombok.Data;
import lombok.NonNull;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Payload for json api response.
 */
@Data
public class ApiResponse {

    /**
     * Return by everything except find*()
     */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private Map<String, Object> status;

    /**
     * If an error ocured
     */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private List<ApiError> errors;

    /**
     * Data retrieved by find*()
     */
    private ApiData data;

    /**
     * Default constructor.
     */
    public ApiResponse() {
    }

    /**
     * Syntax sugar.
     *
     * @param key
     *      key to be retrieved
     * @return
     *      list of values
     */
    @SuppressWarnings("unchecked")
    public Stream<String> getStatusKeyAsStringStream(@NonNull String key) {
        if (status.containsKey(key)) {
            return ((ArrayList<String>) status.get(key)).stream();
        }
        return Stream.empty();
    }

    /**
     * Use when attribute in status is an object.
     *
     * @param key
     *      target get
     * @param targetClass
     *      target class
     * @return
     *      object
     * @param <T>
     *      type in used
     */
    public <T> T getStatusKeyAsObject(@NonNull String key, Class<T> targetClass) {
        if (status.containsKey(key)) {
            return JsonUtils.convertValue(status.get(key), targetClass);
        }
        return null;
    }

    /**
     * Use when attribute in status is a list.
     *
     * @param key
     *      target get
     * @param targetClass
     *      target class
     * @return
     *      object
     * @param <T>
     *      type in used
     */
    public <T> List<T> getStatusKeyAsList(@NonNull String key, Class<T> targetClass) {
        if (status.containsKey(key)) {
            return JsonUtils.getObjectMapper().convertValue(status.get(key),
                   JsonUtils.getObjectMapper().getTypeFactory()
                           .constructCollectionType(List.class, targetClass));
        }
        return null;
    }

    /**
     * Syntax sugar.
     *
     * @param key
     *      key to be retrieved
     * @return
     *      list of values
     */
    public List<String> getStatusKeyAsList(@NonNull String key) {
        return getStatusKeyAsStringStream(key).collect(Collectors.toList());
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
