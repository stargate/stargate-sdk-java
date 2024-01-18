/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.stargate.sdk.data.domain;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import io.stargate.sdk.core.domain.ObjectMap;
import io.stargate.sdk.data.domain.odm.Document;
import io.stargate.sdk.utils.JsonUtils;
import lombok.Data;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

import java.lang.reflect.Array;
import java.time.Instant;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.IntStream;


/**
 * Json Api Payload.
 */
public class JsonDocument extends Document<Map<String, Object>> {

    /**
     * Default Constructor.
     */
    public JsonDocument() {
        super();
        this.data = new HashMap<>();
    }

    /**
     * Public constructor.
     *
     * @param json
     *      json structure
     */
    public JsonDocument(String json) {
        data(json);
        parseIdAndVector();
    }

    /**
     * Export Id and Vector as keys if exists
     */
    private void parseIdAndVector() {
        if (data.containsKey("_id")) {
            this.id = (String) data.get("_id");
            data.remove("_id");
        }
        if (data.containsKey("$vector")) {
            List<Double> doubleList = (List<Double>) data.get("$vector");
            this.vector = new float[doubleList.size()];
            for (int i = 0; i < doubleList.size(); i++) {
                this.vector[i] = doubleList.get(i).floatValue();
            }
            data.remove("$vector");
        }
    }

    /**
     * Public constructor.
     *
     * @param keyValue
     *      key value for the Json
     */
    public JsonDocument(Map<String, Object> keyValue) {
        this.data = keyValue;
        parseIdAndVector();
    }

    /**
     * Fluent getter for document.
     *
     * @param id
     *      id
     * @return
     *      self reference
     */
    @Override
    public JsonDocument id(String id) {
        this.id = id;
        return this;
    }

    /**
     * Fluent getter for document.
     *
     * @param vector
     *      vector
     * @return
     *      self reference
     */
    @Override
    public JsonDocument vector(float[] vector) {
        this.vector = vector;
        return this;
    }

    /**
     * Fluent getter for document.
     *
     * @param data
     *      data
     * @return
     *      self reference
     */
    @Override
    public JsonDocument data(Map<String, Object> data) {
        this.data = data;
        return this;
    }

    /**
     * Fluent getter for document.
     *
     * @param json
     *      json
     * @return
     *      self reference
     */
    @SuppressWarnings("unchecked")
    public JsonDocument data(String json) {
        if (json == null) {
            this.data = null;
        } else {
            this.data = JsonUtils.unmarshallBean((String)json, Map.class);
        }
        return this;
    }

    /**
     * Populate attribute
     *
     * @param key
     *      attribute name
     * @param value
     *      attribute value
     * @return
     *      self reference
     */
    public JsonDocument put(String key, Object value) {
        if (null == data) data = new LinkedHashMap<String, Object>();
        data.put(key, value);
        return this;
    }

    /**
     * Access element from the map
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
        if (data.containsKey(key)) {
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
     * Return a List of items.
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
     *     type of parameters
     */
    public <T> T getObject(String k, Class<T> type) {
        return JsonUtils.convertValueForDataApi(data.get(k), type);
    }

    /**
     * Access element from the map.
     *
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
