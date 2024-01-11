package io.stargate.sdk.data.domain.odm;

import io.stargate.sdk.data.domain.JsonDocument;
import io.stargate.sdk.utils.JsonUtils;
import lombok.Data;

/**
 * Working with entities.
 *
 * @param <T>
 *     type of bean in use
 */
@Data
public class Document<T> {

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
     * Default constructor.
     */
    public Document() {}

    /**
     * Default document.
     *
     * @param record
     *      current json record
     * @param clazz
     *      class
     */
    public Document(JsonDocument record, Class<T> clazz) {
        this.id         = record.getId();
        this.data       = JsonUtils.convertValue(record.getData(), clazz);
        this.vector     = record.getVector();
    }

    /**
     * Default document.
     *
     * @param bean
     *      current payload
     */
    public Document(T bean) {
        this(null, bean, null);
    }

    /**
     * Default document.
     *
     * @param id
     *      identifier
     * @param bean
     *      current payload
     */
    public Document(String id, T bean) {
        this(id, bean, null);
    }

    /**
     * Default document.
     *
     * @param bean
     *      current payload
     * @param vector
     *      vector embeddings
     */
    public Document(T bean, float[] vector) {
        this(null, bean, vector);
    }

    /**
     * Default document.
     *
     * @param id
     *      identifier
     * @param bean
     *      current payload
     * @param vector
     *      vector embeddings
     */
    public Document(String id, T bean, float[] vector) {
        this.id     = id;
        this.data   = bean;
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
     * @param result
     *      json result
     * @param clazz
     *      current class
     * @return
     *      document
     * @param <R>
     *      typed object
     */
    public static <R> Document<R> of(JsonDocument result, Class<R> clazz) {
        return new Document<R>(result, clazz);
    }

    /**
     * Mapping with internal layer.
     *
     * @return
     *      json record
     */
    public JsonDocument toJsonDocument() {
        return new JsonDocument(id, data, vector);
    }
}
