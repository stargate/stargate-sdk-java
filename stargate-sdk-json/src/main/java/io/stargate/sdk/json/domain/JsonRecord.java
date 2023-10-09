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

package io.stargate.sdk.json.domain;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import io.stargate.sdk.core.domain.ObjectMap;
import io.stargate.sdk.utils.JsonUtils;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

import java.util.Arrays;
import java.util.List;
import java.util.Map;


/**
 * Json Api Payload.
 */
@Getter @Setter
public class JsonRecord extends AbstractJsonRecord {

    /**
     * Data for inputs.
     */
    @JsonIgnore
    @JsonAnyGetter
    protected ObjectMap data;

    /**
     * Default constructor.
     */
    public JsonRecord() {}

    /**
     * Constructor with id.
     *
     * @param id
     *      identifier
     */
    public JsonRecord(String id) {
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
    public JsonRecord(String id, Map<String, Object > data) {
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
    public JsonRecord(String id, Object bean) {
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
    public JsonRecord(String id, Map<String, Object > data, List<Float> vector) {
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
    public JsonRecord(String id, Object bean, List<Float> vector) {
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
    public JsonRecord id(String id) {
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
    public JsonRecord put(String key, Object value) {
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
    public JsonRecord data(@NonNull Object bean) {
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
    public JsonRecord data(@NonNull Map<String, Object > map) {
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
    public JsonRecord vector(Float... vector) {
        return vector(Arrays.asList(vector));
    }

    /**
     * Populate vector.
     *
     * @param vector
     *      embeddings
     * @return
     *      self reference
     */
    public JsonRecord vector(List<Float> vector) {
        this.vector = vector;
        return this;
    }


}
