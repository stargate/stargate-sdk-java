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
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Getter;
import lombok.Setter;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * Wrapper for an document retrieved from ASTRA caring a unique identifier.
 */
@Getter @Setter
public class JsonDocument {

    static final ObjectMapper JACKSON_MAPPER = new ObjectMapper();

    @JsonProperty("_id")
    String id;

    @JsonProperty("$vector")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    List<Double> vector;

    @JsonProperty("$vectorize")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    String vectorize;

    @JsonIgnore
    @JsonAnyGetter
    @JsonInclude(JsonInclude.Include.NON_NULL)
    Map<String, Object> document;

    /**
     * Default constructor.
     */
    public JsonDocument() {
    }

    /**
     * Default constructor.
     *
     * @param id
     *      identifier
     */
    public JsonDocument(String id) {
        this.id = id;
    }

    /**
     * Constructor with document builder.
     *
     * @param documentBuilder
     *      builder for the document
     */
    public JsonDocument(DocumentBuilder documentBuilder) {
        this.id = documentBuilder.id;
        this.vector = documentBuilder.vector;
        this.vectorize = documentBuilder.vectorize;
        this.document = documentBuilder.document;
    }

    public JsonDocument vector(Double... vector) {
        this.vector = List.of(vector);
        return this;
    }


    public JsonDocument vector(List<Double> vector) {
        this.vector = vector;
        return this;
    }

    public JsonDocument put(String prop, Object value) {
        if (null == document) {
            document = new HashMap<>();
        }
        document.put(prop, value);
        return this;
    }

    @SuppressWarnings("unchecked")
    public <T> JsonDocument document(T t) {
        this.document = JACKSON_MAPPER.convertValue(t, Map.class);
        return this;
    }

    @SuppressWarnings("unchecked")
    public JsonDocument jsonDocument(String t) {
        try {
            this.document = JACKSON_MAPPER.readValue(t, Map.class);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Cannot parse json", e);
        }
        return this;
    }

    public static DocumentBuilder builder() {
        return new DocumentBuilder();
    }

    public static class DocumentBuilder {

        String id;

        List<Double> vector;

        String vectorize;

        Map<String, Object> document;

        public DocumentBuilder id(String id) {
            this.id = id;
            return this;
        }

        public DocumentBuilder vectorize(String vectorize) {
            this.vectorize = vectorize;
            return this;
        }

        public DocumentBuilder vector(Double... vector) {
            this.vector = List.of(vector);
            return this;
        }
        public DocumentBuilder vector(List<Double> vector) {
            this.vector = vector;
            return this;
        }

        @SuppressWarnings("unchecked")
        public <T> DocumentBuilder document(T t) {
            this.document = JACKSON_MAPPER.convertValue(t, Map.class);
            return this;
        }

        public DocumentBuilder put(String prop, Object value) {
            if (null == document) {
                document = new HashMap<>();
            }
            document.put(prop, value);
            return this;
        }

        @SuppressWarnings("unchecked")
        public DocumentBuilder jsonDocument(String t) {
            try {
                this.document = JACKSON_MAPPER.readValue(t, Map.class);
            } catch (JsonProcessingException e) {
                throw new IllegalArgumentException("Cannot parse json", e);
            }
            return this;
        }

        public JsonDocument build() {
            return new JsonDocument(this);
        }

    }

}
