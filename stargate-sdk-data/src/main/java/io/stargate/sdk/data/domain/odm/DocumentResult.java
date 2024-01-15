package io.stargate.sdk.data.domain.odm;

import com.fasterxml.jackson.annotation.JsonAnySetter;
import io.stargate.sdk.data.domain.JsonDocumentResult;
import io.stargate.sdk.utils.JsonUtils;
import lombok.Getter;
import lombok.Setter;

import java.util.Map;


/**
 * Result class with ODM.
 *
 * @param <DOC>
 *     pojo in use for ODM
 */
public class DocumentResult<DOC> extends Document<DOC> {

    /**
     * Using an object, can be null
     */
    @Getter @Setter
    protected Float similarity;

    /**
     * Default constructor.
     */
    public DocumentResult() {}

    /**
     * Default constructor.
     *
     * @param result
     *     copy constructor
     */
    public DocumentResult(JsonDocumentResult result) {
        this.id         = result.getId();
        this.vector     = result.getVector();
        this.similarity = result.getSimilarity();
    }

    /**
     * Default constructor
     *
     * @param result
     *      json result
     * @param data
     *      payload
     */
    @SuppressWarnings("unchecked")
    public DocumentResult(JsonDocumentResult result, DOC data) {
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
    public DocumentResult(JsonDocumentResult result, Class<DOC> clazz) {
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
    public static <R> DocumentResult<R> of(JsonDocumentResult result, Class<R> clazz) {
        return new DocumentResult<>(result, clazz);
    }

}
