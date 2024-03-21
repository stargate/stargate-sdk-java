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

package io.stargate.sdk.core.domain;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.introspect.JacksonAnnotationIntrospector;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.stargate.sdk.serialization.CustomEJsonCalendarDeserializer;
import io.stargate.sdk.serialization.CustomEJsonCalendarSerializer;
import io.stargate.sdk.serialization.CustomEJsonDateDeserializer;
import io.stargate.sdk.serialization.CustomEJsonDateSerializer;
import io.stargate.sdk.serialization.CustomEJsonInstantDeserializer;
import io.stargate.sdk.serialization.CustomEJsonInstantSerializer;
import io.stargate.sdk.serialization.CustomObjectIdDeserializer;
import io.stargate.sdk.serialization.CustomObjectIdSerializer;
import io.stargate.sdk.serialization.CustomUuidDeserializer;
import io.stargate.sdk.serialization.CustomUuidSerializer;
import io.stargate.sdk.serialization.CustomUuidv6Deserializer;
import io.stargate.sdk.serialization.CustomUuidv6Serializer;
import io.stargate.sdk.serialization.CustomUuidv7Deserializer;
import io.stargate.sdk.serialization.CustomUuidv7Serializer;
import io.stargate.sdk.types.ObjectId;
import io.stargate.sdk.types.UUIDv6;
import io.stargate.sdk.types.UUIDv7;
import lombok.NonNull;

import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

/**
 * Wrapper to parse Rows as an HashMap.
 * 
 * @author Cedrick LUNVEN (@clunven)
 */
public class ObjectMap extends HashMap<String, Object> {

    /** object mapper. */
    private static ObjectMapper objectMapper;

    /**
     * Initialize object mapper.
     *
     * @return
     *      mapper
     */
    public static synchronized ObjectMapper getObjectMapper() {
        if (objectMapper == null) {
            objectMapper = new ObjectMapper()
                    .configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true)
                    .configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true)
                    .configure(MapperFeature.ACCEPT_CASE_INSENSITIVE_PROPERTIES, true)
                    .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
                    .configure(DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES, false)
                    .configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false)
                    .registerModule(new JavaTimeModule())
                    .setDateFormat(new SimpleDateFormat("dd/MM/yyyy"))
                    .setSerializationInclusion(JsonInclude.Include.NON_NULL)
                    .setAnnotationIntrospector(new JacksonAnnotationIntrospector());

            SimpleModule module = new SimpleModule();
            // Date
            module.addSerializer(Date.class, new CustomEJsonDateSerializer());
            module.addDeserializer(Date.class, new CustomEJsonDateDeserializer());
            // Calendar
            module.addSerializer(Calendar.class, new CustomEJsonCalendarSerializer());
            module.addDeserializer(Calendar.class, new CustomEJsonCalendarDeserializer());
            // Instant
            module.addSerializer(Instant.class, new CustomEJsonInstantSerializer());
            module.addDeserializer(Instant.class, new CustomEJsonInstantDeserializer());
            // UUID
            module.addSerializer(UUID.class, new CustomUuidSerializer());
            module.addDeserializer(UUID.class, new CustomUuidDeserializer());
            // UUIDv6
            module.addSerializer(UUIDv6.class, new CustomUuidv6Serializer());
            module.addDeserializer(UUIDv6.class, new CustomUuidv6Deserializer());
            // UUIDv7
            module.addSerializer(UUIDv7.class, new CustomUuidv7Serializer());
            module.addDeserializer(UUIDv7.class, new CustomUuidv7Deserializer());
            // ObjectId
            module.addSerializer(ObjectId.class, new CustomObjectIdSerializer());
            module.addDeserializer(ObjectId.class, new CustomObjectIdDeserializer());
            objectMapper.registerModule(module);
        }
        return objectMapper;
    }

    /** Serial. */
    private static final long serialVersionUID = 3279531139420446635L;

    /**
     * Default constructor
     */
    public ObjectMap() {
        super();
    }

    /**
     * Copy constructor.
     *
     * @param map
     *      current Map
     */
    public ObjectMap(Map<String, Object> map) {
        super(map);
    }

    /**
     * Access element from the map
     * @param key
     *      current configuration key
     * @param type
     *      configuration type
     * @param required
     *      if the key is required
     * @return
     *      configuration value
     * @param <K>
     *     type f parameters
     */
    @SuppressWarnings("unchecked")
    public <K> K get(String key, Class<K> type, boolean required) {
        Objects.requireNonNull(type, "Type is required");
        if (containsKey(key)) {
            if (type.isAssignableFrom(get(key).getClass())) {
                return (K) get(key);
            }
            throw new IllegalArgumentException("Argument " + key + " is not a " + type.getSimpleName());
        }
        if (required) {
            throw new IllegalArgumentException("Argument " + key + " is required but was not found");
        }
        return null;
    }

    @SuppressWarnings("unchecked")
    public <K> Set<K> getSet(String key, Class<K> itemClass) {
        return (Set<K>) this.get(key, Set.class, true);
    }

    @SuppressWarnings("unchecked")
    public <K> List<K> getList(String k, Class<K> itemClass) {
        return (List<K>) this.get(k, List.class, true);
    }

    /**
     * Access element from the map
     * @param k
     *      current configuration key
     * @return
     *      configuration value
     */
    public String getString(String k) {
        return get(k, String.class, true);
    }

    /**
     * Access element from the map
     * @param k
     *      current configuration key
     * @return
     *      configuration value
     */
    public Double getDouble(String k) {
        return get(k, Double.class, true);
    }

    /**
     * Access element from the map
     * @param k
     *      current configuration key
     * @return
     *      configuration value
     */
    public Integer getInteger(String k) {
        return get(k, Integer.class, true);
    }

    /**
     * Access element from the map
     * @param k
     *      current configuration key
     * @return
     *      configuration value
     */
    public Boolean getBoolean(String k) {
        return get(k, Boolean.class, true);
    }

    /**
     * Access element from the map
     * @param k
     *      current configuration key
     * @return
     *      configuration value
     */
    public Float getFloat(String k) {
        return get(k, Float.class, true);
    }

    /**
     * Access element from the map
     * @param k
     *      current configuration key
     * @return
     *      configuration value
     */
    public Long getLong(String k) {
        return get(k, Long.class, true);
    }

    /**
     * Access element from the map
     * @param k
     *      current configuration key
     * @return
     *      configuration value
     */
    public Short getShort(String k) {
        return get(k, Short.class, true);
    }

    /**
     * Access element from the map
     * @param k
     *      current configuration key
     * @return
     *      configuration value
     */
    public Byte getByte(String k) {
        return get(k, Byte.class, true);
    }

    /**
     * Access element from the map
     * @param k
     *      current configuration key
     * @return
     *      configuration value
     */
    public Character getCharacter(String k) {
        return get(k, Character.class, true);
    }

    /**
     * Convert if possible.
     *
     * @param o
     *      current object
     * @return
     *      object map
     */
    public static ObjectMap of(@NonNull  Object o) {
        return getObjectMapper().convertValue(o, ObjectMap.class);
    }
}
