package io.stargate.sdk.data.domain.odm;

import io.stargate.sdk.data.domain.JsonDocument;
import io.stargate.sdk.data.domain.JsonResult;
import io.stargate.sdk.utils.JsonUtils;
import lombok.Getter;
import lombok.Setter;


/**
 * Result class with ODM.
 *
 * @param <DOC>
 *     pojo in use for ODM
 */
public class Result<DOC> extends Document<DOC> {

    /**
     * Using an object, can be null
     */
    @Getter @Setter
    protected Float similarity;

    /**
     * Default constructor.
     */
    public Result() {}

    /**
     * Default constructor.
     *
     * @param result
     *     copy constructor
     */
    public Result(JsonResult result) {
        this.id         = result.getId();
        this.vector     = result.getVector();
        this.similarity = result.getSimilarity();
    }

    /**
     * Default constructor.
     */
    public Result(JsonResult result, DOC data) {
        this.id         = result.getId();
        this.vector     = result.getVector();
        this.similarity = result.getSimilarity();
        this.data       = data;
    }

    /**
     * Constructor.
     *
     * @param result
     *      json result
     * @param clazz
     *      class to convert into
     */
    public Result(JsonResult result, Class<DOC> clazz) {
        this(result, JsonUtils.convertValueForDataApi(result.getData(), clazz));
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
