package io.stargate.sdk.json.vector;

import io.stargate.sdk.core.domain.Page;
import io.stargate.sdk.json.JsonCollectionRepository;
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
import java.util.List;
import java.util.Optional;

/**
 * Crud Repository for Vector entities
 *
 * @param <BEAN>
 *     curren vector object
 */
public class VectorCollectionRepository<BEAN> extends JsonCollectionRepository<BEAN> {

    /**
     * Default constructor.
     *
     * @param col
     *      collection client parent
     * @param clazz
     *      working bean class
     */
    public VectorCollectionRepository(JsonCollectionClient col, Class<BEAN> clazz) {
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
     *      vector to provide
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
     * @param vector
     *      embeddings
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
     *
     * @param id
     *      identifier
     * @param current
     *      current bean
     * @param vector
     *      embeddings
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
     * Find by vector
     *
     * @param vector
     *      vector
     * @return
     *      object if presents
     */
    public Optional<Result<BEAN>> findByVector(@NonNull float[] vector) {
        return collectionClient.findOneByVector(vector, docClass);
    }

    /**
     * Delete by vector
     *
     * @param vector
     *      vector
     * @return
     *      if object deleted
     */
    public boolean deleteByVector(float[] vector) {
        return collectionClient.deleteByVector(vector) > 0;
    }

    // ------------------------------
    // ---  Similarity Search    ----
    // ------------------------------

    /**
     * Search similarity from the vector (page by 20)
     *
     * @param vector
     *      vector
     * @return
     *      page of results
     */
    public Page<Result<BEAN>> similaritySearch(float[] vector) {
        return similaritySearch(vector, null, null, null);
    }

    /**
     * Search similarity from the vector (page by 20)
     *
     * @param vector
     *      vector
     * @param metadataFilter
     *      metadata filtering
     * @return
     *      page of results
     */
    public Page<Result<BEAN>> similaritySearch(float[] vector, Filter metadataFilter) {
        return similaritySearch(vector, metadataFilter, null, null);
    }

    /**
     * Search similarity from the vector for another page
     *
     * @param vector
     *      vector
     * @param pagingState
     *      paging state for different page
     * @return
     *      page of results
     */
    public Page<Result<BEAN>> similaritySearch(float[] vector, String pagingState) {
        return similaritySearch(vector, null, null, pagingState);
    }

    /**
     * Search similarity from the vector (page by 20)
     *
     * @param vector
     *      vector
     * @param metadataFilter
     *      metadata filtering
     * @param pagingState
     *      paging state for different page
     * @return
     *      page of results
     */
    public Page<Result<BEAN>> similaritySearch(float[] vector, Filter metadataFilter, String pagingState) {
        return similaritySearch(vector, metadataFilter, null, pagingState);
    }

    /**
     * Search similarity from the vector and a limit, if a limit / no paging
     *
     * @param vector
     *      vector
     * @param limit
     *      return count
     * @return
     *      page of results
     */
    public List<Result<BEAN>> similaritySearch(float[] vector, Integer limit) {
        return similaritySearch(vector, null, limit, null).getResults();
    }

    /**
     * Search similarity from the vector and a limit, if a limit / no paging
     *
     * @param vector
     *      vector
     * @param limit
     *      return count
     * @param metadataFilter
     *      metadata filtering
     * @return
     *      page of results
     */
    public List<Result<BEAN>> similaritySearch(float[] vector, Filter metadataFilter, Integer limit) {
        return similaritySearch(vector, metadataFilter, limit, null).getResults();
    }

    /**
     * Query builder.
     *
     * @param vector
     *      vector embeddings
     * @param filter
     *      metadata filter
     * @param limit
     *      limit
     * @param pagingState
     *      paging state
     * @return
     *      result page
     */
    private Page<Result<BEAN>> similaritySearch(float[] vector, Filter filter, Integer limit, String pagingState) {
        SelectQueryBuilder builder = SelectQuery.builder().orderByAnn(vector);
        if (filter != null) {
            if (builder.filter == null) {
                builder.filter = new HashMap<>();
            }
            builder.filter.putAll(filter.getFilter());
        }
        builder.includeSimilarity();
        if (pagingState != null) {
            builder.withPagingState(pagingState);
        }
        if (limit!=null) {
            builder.limit(limit);
        }
        return collectionClient.queryForPage(builder.build(), docClass);
    }

}



