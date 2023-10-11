package io.stargate.sdk.json.vector;

import io.stargate.sdk.core.domain.Page;
import io.stargate.sdk.json.CollectionRepository;
import io.stargate.sdk.json.JsonCollectionClient;
import io.stargate.sdk.json.domain.Filter;
import io.stargate.sdk.json.domain.JsonDocument;
import io.stargate.sdk.json.domain.JsonResultUpdate;
import io.stargate.sdk.json.domain.SelectQuery;
import io.stargate.sdk.json.domain.SelectQueryBuilder;
import io.stargate.sdk.json.domain.UpdateQuery;
import io.stargate.sdk.json.domain.odm.Result;
import lombok.NonNull;

import java.util.HashMap;
import java.util.Optional;

/**
 * Crud Repository for Vector entities
 *
 * @param <BEAN>
 *     curren vector object
 */
public class VectorStore<BEAN> extends CollectionRepository<BEAN> {

    /**
     * Default constructor.
     *
     * @param col
     *      collection client parent
     * @param clazz
     *      working bean class
     */
    public VectorStore(JsonCollectionClient col, Class<BEAN> clazz) {
       super(col, clazz);
    }

    // --------------------------
    // ---      Insert       ----
    // --------------------------

    /**
     * Save a NEW RECORD with a defined id.
     * @param id
     *      provided id
     * @param current
     *      current bean to update
     * @param vector
     *      vector to privide
     * @return
     *      generated identifier
     */
    public String insert(String id, @NonNull BEAN current, float[] vector) {
        return collectionClient.insertOne(id, current, vector);
    }

    /**
     * Save a NEW RECORD that generate an id.
     *
     * @param current
     *      current bean to update
     * @param vector
     *      vectorcd
     * @return
     *      generated identifier
     */
    public String insert(@NonNull BEAN current, float[] vector) {
        return collectionClient.insertOne(current, vector);
    }

    // --------------------------
    // ---      save         ----
    // --------------------------

    /**
     * Generate a new document with a new id.
     *
     * @param current
     *      current object to create
     * @return
     *      generated id
     */
    public final String save(@NonNull BEAN current, float[] vector) {
        return insert(current, vector);
    }

    /**
     * Create a new document with the id, if I already exist an error will occur.
     * @param id
     *      identifier
     * @param current
     *      current bean
     * @return
     *      if the document has been updated
     */
    public boolean save(String id, @NonNull BEAN current, float[] vector) {
        if (!exists(id)) {
            insert(id, current, vector);
            return true;
        }
        JsonResultUpdate res = collectionClient
                .findOneAndReplace(UpdateQuery.builder()
                        .where("_id")
                        .isEqualsTo(id)
                        .replaceBy(new JsonDocument(id, current, vector))
                        .build());
        return res.getUpdateStatus().getModifiedCount() > 0;
    }

    // --------------------------
    // --- find*             ----
    // --------------------------

    /**
     * Find by Id.
     *
     * @param embeddings
     *      embeddings
     * @return
     *      object if presents
     */
    public Optional<Result<BEAN>> findByVector(@NonNull float[] embeddings) {
        return collectionClient.findOneByVector(embeddings, docClass);
    }

    public boolean deleteByVector(float[] vector) {
        return collectionClient.deleteByVector(vector) > 0;
    }

    public Page<Result<BEAN>> similaritySearch(float[] embeddings) {
        return similaritySearch(embeddings, null, null);
    }

    public Page<Result<BEAN>> similaritySearch(float[] embeddings, Integer limit) {
        return similaritySearch(embeddings, null, limit);
    }

    public Page<Result<BEAN>> similaritySearch(float[] embeddings, Filter filter, Integer limit) {
        SelectQueryBuilder builder = SelectQuery.builder().orderByAnn(embeddings);
        if (filter != null) {
            if (builder.filter == null) {
                builder.filter = new HashMap<>();
            }
            builder.filter.putAll(filter.getFilter());
        }
        builder.withIncludeSimilarity();
        if (limit!=null) {
            builder.limit(limit);
        }
        return collectionClient.queryForPage(builder.build(), docClass);
    }

}


