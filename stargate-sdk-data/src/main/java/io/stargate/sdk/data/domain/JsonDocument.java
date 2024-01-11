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
import io.stargate.sdk.utils.JsonUtils;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

import java.util.Map;


/**
 * Json Api Payload.
 */
@Getter @Setter
public class JsonDocument extends AbstractDocument {

    /**
     * Data for inputs.
     */
    @JsonIgnore
    @JsonAnyGetter
    protected ObjectMap data;

    /**
     * Default constructor.
     */
    public JsonDocument() {}

    /**
     * Constructor with id.
     *
     * @param id
     *      identifier
     */
    public JsonDocument(String id) {
        this(id, null, null);
    }

    /**
     * Constructor with id and data (map)
     *
     * @param id
     *      identifier
     * @param data
     *      data as a map
     */
    public JsonDocument(String id, Map<String, Object > data) {
        this(id, data, null);
    }

    /**
     * Constructor with id and data (map
     *
     * @param id
     *      identifier
     * @param bean
     *      data as a beam
     */
    public JsonDocument(String id, Object bean) {
        this(id, bean, null);
    }

    /**
     * Constructor with id and data (map)
     *
     * @param id
     *      identifier
     * @param data
     *      data as a map
     * @param vector
     *      vector
     */
    public JsonDocument(String id, Map<String, Object > data, float[] vector) {
        this.id   = id;
        this.vector = vector;
        this.data = asObjectMap(data);
    }

    /**
     * Constructor with id and data (beam)
     *
     * @param id
     *      identifier
     * @param bean
     *      data as a beam
     * @param vector
     *      vector
     */
    public JsonDocument(String id, Object bean, float[] vector) {
        this.id = id;
        this.vector = vector;
        this.data = asObjectMap(bean);
    }

    /**
     * Convert as Object Map
     *
     * @param map
     *      current map
     */
    private ObjectMap asObjectMap(Map<String, Object > map)  {
        if (map == null) return null;
        ObjectMap objectMap = new ObjectMap();
        objectMap.putAll(map);
        return objectMap;
    }

    /**
     * Convert as Object Map
     *
     * @param bean
     *      current bean
     */
    private ObjectMap asObjectMap(Object bean)  {
        if (bean == null) return null;
        if (bean instanceof String) {
            bean = JsonUtils.unmarshallBean((String)bean, Map.class);
        }
        return JsonUtils.convertValue(bean, ObjectMap.class);
    }

    /**
     * Populate id.
     *
     * @param id
     *      identifier
     * @return
     *      new JsonRecord
     */
    public JsonDocument id(String id) {
        this.id = id;
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
        if (null == data) data = new ObjectMap();
        data.put(key, value);
        return this;
    }

    /**
     * Populate data.
     *
     * @param bean
     *      data as beam
     * @return
     *      self reference
     */
    public JsonDocument data(@NonNull Object bean) {
        this.data = asObjectMap(bean);
        return this;
    }

    /**
     * Populate data.
     *
     * @param map
     *      data as map
     * @return
     *      self reference
     */
    public JsonDocument data(@NonNull Map<String, Object > map) {
        this.data = asObjectMap(map);
        return this;
    }

    /**
     * Populate vector.
     *
     * @param vector
     *      embeddings
     * @return
     *      self reference
     */
    public JsonDocument vector(float[] vector) {
        this.vector = vector;
        return this;
    }


}
