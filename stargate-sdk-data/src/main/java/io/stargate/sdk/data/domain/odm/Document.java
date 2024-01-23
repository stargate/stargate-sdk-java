package io.stargate.sdk.data.domain.odm;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonUnwrapped;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.stargate.sdk.data.domain.JsonDocument;
import io.stargate.sdk.utils.JsonUtils;
import lombok.Data;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import javax.print.Doc;

/**
 * Unitary entity for a document. Adding `id` and `vector`.
 *
 * @param <T>
 *     type of bean in use
 */
@JsonSerialize(using = DocumentSerializer.class)
public class Document<T> {

    /**
     * Row id for a vector
     */
    @JsonProperty("_id")
    @Getter @Setter
    protected String id;

    /**
     * Embeddings
     */
    @JsonProperty("$vector")
    @Getter @Setter
    protected float[] vector;

    /**
     * Metadata for a vector
     */
    @Getter @Setter
    // @JsonUnwrapped -> Not working, moving to custom serializer
    protected T data;

    /**
     * Default Document
     */
    public Document() {
    }

    /**
     * Full Constructor
     * @param id
     *      identifier
     * @param data
     *      data
     * @param vector
     *      vector
     */
    public Document(String id, T data, float[] vector) {
        this.id = id;
        this.data = data;
        this.vector = vector;
    }

    /**
     * Fluent getter for document.
     *
     * @param id
     *      id
     * @return
     *      self reference
     */
    public Document<T> id(String id) {
        this.id = id;
        return this;
    }

    /**
     * Fluent getter for document.
     *
     * @param vector
     *      vector
     * @return
     *      self reference
     */
    public Document<T> vector(float[] vector) {
        this.vector = vector;
        return this;
    }

    /**
     * Fluent getter for document.
     *
     * @param data
     *      data
     * @return
     *      self reference
     */
    public Document<T> data(T data) {
        this.data = data;
        return this;
    }

    /**
     * Static initialization.
     *
     * @param id
     *      identifier
     * @param vector
     *      vector
     * @param data
     *      payload
     * @return
     *      document
     * @param <R>
     *      typed object
     */
    public static <R> Document<R> of(String id, float[] vector, R data) {
        return new Document<R>().id(id).vector(vector).data(data);
    }

    /**
     * Print the document as a Json String.
     *
     * @return
     *      json string
     */
    @Override
    public String toString() {
        return JsonUtils.marshallForDataApi(this);
    }
}
