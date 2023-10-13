package io.stargate.sdk.json.domain.odm;

import io.stargate.sdk.json.domain.JsonDocument;
import io.stargate.sdk.json.domain.JsonResult;
import io.stargate.sdk.utils.JsonUtils;
import lombok.Getter;


/**
 * Result class with ODM.
 *
 * @param <T>
 *     pojo in use for ODM
 */
public class Result<T> extends Document<T> {

    /**
     * Using an object, can be null
     */
    @Getter
    protected Float similarity;

    /**
     * Default constructor.
     */
    public Result() {}

    /**
     * Constructor.
     *
     * @param result
     *      json result
     * @param clazz
     *      class to convert into
     */
    public Result(JsonResult result, Class<T> clazz) {
        this.id         = result.getId();
        this.data       = JsonUtils.convertValue(result.getData(), clazz);
        this.vector     = result.getVector();
        this.similarity = result.getSimilarity();
    }

    /**
     * Syntax sugar to build a result.
     *
     * @param result
     *      json result
     * @param clazz
     *      class to convert
     * @return
     *      instance of result
     * @param <R>
     *     pojo in use for ODM
     */
    public static <R> Result<R> of(JsonResult result, Class<R> clazz) {
        return new Result<>(result, clazz);
    }

    /**
     * Mapping with internal layer.
     *
     * @return
     *      json record
     */
    public JsonResult toJsonResult() {
        JsonDocument doc  = new JsonDocument(id, data, vector);
        JsonResult result = new JsonResult();
        result.setId(doc.getId());
        result.setVector(doc.getVector());
        result.setData(doc.getData());
        result.setSimilarity(similarity);
        return result;
    }

}
