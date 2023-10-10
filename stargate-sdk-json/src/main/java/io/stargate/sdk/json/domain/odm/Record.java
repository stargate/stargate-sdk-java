package io.stargate.sdk.json.domain.odm;

import io.stargate.sdk.json.domain.JsonRecord;
import io.stargate.sdk.json.domain.JsonResult;
import io.stargate.sdk.utils.JsonUtils;
import lombok.Data;

import java.util.List;

/**
 * Working with entities.
 *
 * @param <T>
 *     type of bean in use
 */
@Data
public class Record<T> {

    /**
     * Row id for a vector
     */
    protected String id;

    /**
     * Metadata for a vector
     */
    protected T data;

    /**
     * Embeddings
     */
    protected float[] vector;

    /**
     * Similarity
     */
    protected Float similarity;

    public Record(JsonResult result, Class<T> clazz) {
        this.id         = result.getId();
        this.data       = JsonUtils.convertValue(result.getData(), clazz);
        this.vector     = result.getVector();
        this.similarity = result.getSimilarity();
    }

    /**
     * Mapping with internal layer.
     *
     * @return
     *      json record
     */
    public JsonRecord asJsonRecord() {
        return new JsonRecord(id, data, vector);
    }
}
