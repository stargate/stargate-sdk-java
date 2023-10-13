package io.stargate.sdk.json.domain;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.sdk.utils.JsonUtils;

import java.util.List;

/**
 * Super class for all Json Record.
 */
public abstract class AbstractJsonRecord {

    /**
     * Unique identifier.
     */
    @JsonProperty("_id")
    protected String id;

    /**
     * Embeddings vector.
     */
    @JsonProperty("$vector")
    protected float[] vector;

    /**
     * Default constructor.
     */
    public AbstractJsonRecord() {
    }

    /**
     * Gets id
     *
     * @return value of id
     */
    public String getId() {
        return id;
    }

    /**
     * Gets vector
     *
     * @return value of vector
     */
    public float[] getVector() {
        return vector;
    }

    /**
     * Set value for id
     *
     * @param id new value for id
     */
    public void setId(String id) {
        this.id = id;
    }

    /**
     * Set value for vector
     *
     * @param vector new value for vector
     */
    public void setVector(float[] vector) {
        this.vector = vector;
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return JsonUtils.marshall(this);
    }
}
