package io.stargate.sdk.json.domain.odm;

import io.stargate.sdk.json.domain.JsonResult;
import io.stargate.sdk.utils.JsonUtils;

public class Result<T> extends Document<T> {

    /**
     * Using an object, can be null
     */
    protected Float similarity;

    /**
     * Default constructor.
     */
    public Result() {}

    public Result(JsonResult result, Class<T> clazz) {
        this.id         = result.getId();
        this.data       = JsonUtils.convertValue(result.getData(), clazz);
        this.vector     = result.getVector();
        this.similarity = result.getSimilarity();
    }

    public static <R> Result<R> of(JsonResult result, Class<R> clazz) {
        return new Result<>(result, clazz);
    }

    /**
     * Gets similarity
     *
     * @return value of similarity
     */
    public Float getSimilarity() {
        return similarity;
    }
}
