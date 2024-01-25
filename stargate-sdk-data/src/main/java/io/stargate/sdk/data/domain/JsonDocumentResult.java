package io.stargate.sdk.data.domain;

import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.stargate.sdk.data.domain.odm.DocumentResult;
import io.stargate.sdk.utils.JsonUtils;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;

import java.lang.reflect.Array;
import java.time.Instant;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

/**
 * Represents entity returns for find() queries working with shemaless documents.
 */
@Getter @Setter
public class JsonDocumentResult extends DocumentResult<Map<String, Object>> {

    /**
     * Output as a map (to use JsonAySetter annotation).
     */
    @JsonAnySetter
    protected Map<String, Object> jsonRawData;

    /**
     * Default constructor.
     */
    public JsonDocumentResult() {
    }

    /**
     * For a schemaless document you might want to overrride.
     *
     * @return
     *      similarity
     */
    @Override
    public Float getSimilarity() {
        return (similarity == null) ? getFloat("$similarity") : similarity;
    }

    /**
     * Access internal data.
     *
     * @return
     *      map of object
     */
    @Override
    @SuppressWarnings("unchecked")
    public Map<String, Object> getData() {
        if (data == null) {
            data = JsonUtils.convertValueForDataApi(jsonRawData, Map.class);
        }
        return super.getData();
    }

    /**
     * Access element from the map.
     *
     * @param key
     *      current configuration key
     * @param type
     *      configuration type
     * @return
     *      configuration value
     * @param <K>
     *     type f parameters
    @ */
    @SuppressWarnings("unchecked")
    private <K> K get(String key, Class<K> type) {
        Objects.requireNonNull(type, "Type is required");
        if (getData().containsKey(key)) {
            if (type.isAssignableFrom(data.get(key).getClass())) {
                return (K) data.get(key);
            }
            // Integer -> Long
            if (type.equals(Long.class) && data.get(key) instanceof Integer) {
                return (K) Long.valueOf((Integer) data.get(key));
            }
            // Integer -> Short
            if (type.equals(Short.class) && data.get(key) instanceof Integer) {
                return (K) (Short) ((Integer) data.get(key)).shortValue();
            }
            // Integer -> Byte
            if (type.equals(Byte.class) && data.get(key) instanceof Integer) {
                return (K) (Byte) ((Integer) data.get(key)).byteValue();
            }
            // Double -> Float
            if (type.equals(Float.class) && data.get(key) instanceof Double) {
                return (K) (Float) ((Double) data.get(key)).floatValue();
            }
            // String -> Character
            if (type.equals(Character.class) && data.get(key) instanceof String) {
                return (K) (Character) ((String) data.get(key)).charAt(0);
            }
            // String -> UUID
            if (type.equals(UUID.class) && data.get(key) instanceof String) {
                return (K) UUID.fromString((String) data.get(key));
            }

            throw new IllegalArgumentException("Argument '" + key + "' is not a " + type.getSimpleName() + " but a "
                    + data.get(key).getClass().getSimpleName());
        }
        return null;
    }

    /**
     * Return an Array of items.
     *
     * @param k
     *      key
     * @param itemClass
     *      expected class
     * @return
     *      list of items
     * @param <K>
     *      type of item
     */
    @SuppressWarnings("unchecked")
    public <K> List<K> getList(String k, Class<K> itemClass) {
        return (List<K>) this.get(k, List.class);
    }

    /**
     * Return an Array of items.
     *
     * @param k
     *      key
     * @param itemClass
     *      expected class
     * @return
     *      list of items
     * @param <K>
     *      type of item
     */
    @SuppressWarnings("unchecked")
    public <K> K[] getArray(String k, Class<K> itemClass) {
        List<K> list = getList(k, itemClass);
        K[] array = (K[]) Array.newInstance(itemClass, list.size());
        return list.toArray(array);
    }

    /**
     * Access element from the map.
     *
     * @param k
     *      current configuration key
     * @param type
     *      type of elements
     * @return
     *      configuration value
     * @param <T>
     *     type f parameters
     */
    public <T> T getObject(String k, Class<T> type) {
        return JsonUtils.convertValueForDataApi(getData().get(k), type);
    }

    /**
     * Access element from the map
     * @param k
     *      current configuration key
     * @return
     *      configuration value
     */
    public String getString(String k) {
        return get(k, String.class);
    }

    /**
     * Access element from the map
     * @param k
     *      current configuration key
     * @return
     *      configuration value
     */
    public Double getDouble(String k) {
        return get(k, Double.class);
    }

    /**
     * Access element from the map
     * @param k
     *      current configuration key
     * @return
     *      configuration value
     */
    public Integer getInteger(String k) {
        return get(k, Integer.class);
    }

    /**
     * Access element from the map
     * @param k
     *      current configuration key
     * @return
     *      configuration value
     */
    public Boolean getBoolean(String k) {
        return get(k, Boolean.class);
    }

    /**
     * Access element from the map
     * @param k
     *      current configuration key
     * @return
     *      configuration value
     */
    public UUID getUUID(String k) {
        String uuid = getString(k);
        return (uuid == null) ? null : UUID.fromString(uuid);
    }

    /**
     * Access element from the map
     * @param k
     *      current configuration key
     * @return
     *      configuration value
     */
    public Float getFloat(String k) {
        return get(k, Float.class);
    }

    /**
     * Access element from the map
     * @param k
     *      current configuration key
     * @return
     *      configuration value
     */
    public Long getLong(String k) {

        return get(k, Long.class);
    }

    /**
     * Access element from the map
     * @param k
     *      current configuration key
     * @return
     *      configuration value
     */
    public Short getShort(String k) {
        return get(k, Short.class);
    }

    /**
     * Access element from the map
     * @param k
     *      current configuration key
     * @return
     *      configuration value
     */
    public Byte getByte(String k) {
        return get(k, Byte.class);
    }

    /**
     * Access element from the map
     * @param k
     *      current configuration key
     * @return
     *      configuration value
     */
    public Character getCharacter(String k) {
        return  get(k, Character.class);
    }

    /**
     * Access element from the map
     * @param k
     *      current configuration key
     * @return
     *      configuration value
     */
    public Date getDate(String k) {
        return getObject(k, Date.class);
    }

    /**
     * Access element from the map
     * @param k
     *      current configuration key
     * @return
     *      configuration value
     */
    public Calendar getCalendar(String k) {
        return getObject(k, Calendar.class);
    }

    /**
     * Access element from the map
     * @param k
     *      current configuration key
     * @return
     *      configuration value
     */
    public Instant getInstant(String k) {
        return getObject(k, Instant.class);
    }

    /**
     * Access element from the map
     * @param key
     *      current configuration key
     * @return
     *      if key exists
     */
    public boolean isAttributeExist(String key) {
        return data.containsKey(key);
    }

}
