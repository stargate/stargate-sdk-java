package io.stargate.sdk.json.vector;

import io.stargate.sdk.core.domain.ObjectMap;
import io.stargate.sdk.core.domain.Page;
import io.stargate.sdk.json.JsonCollectionClient;
import io.stargate.sdk.json.domain.Filter;
import io.stargate.sdk.json.domain.JsonDocument;
import io.stargate.sdk.json.domain.JsonResult;
import io.stargate.sdk.json.domain.SelectQuery;
import io.stargate.sdk.json.domain.odm.Document;
import io.stargate.sdk.json.domain.odm.Result;
import lombok.NonNull;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Generic Store.
 */
public class JsonVectorStore extends VectorStore<ObjectMap> {

    /**
     * Default constructor.
     *
     * @param col   collection client parent
     */
    public JsonVectorStore(JsonCollectionClient col) {
        super(col, ObjectMap.class);
    }

    // =========================================================
    //                    Map    Find*
    // =========================================================



    /**
     * Find by vector Json
     *
     * @param vector
     *      document embeddings
     * @return
     *      document
     */
    public Optional<JsonResult> findByVectorJson(@NonNull float[] vector) {
        return findByVector(vector).map(Result::toJsonResult);
    }

    // =========================================================
    //                    Map Similarity Searches
    // =========================================================

    /**
     * Search similarity from the vector (page by 20)
     *
     * @param vector
     *      vector
     * @return
     *      page of results
     */
    public Page<JsonResult> similaritySearchJson(float[] vector) {
        return mapAsPageJsonResult(similaritySearch(vector));
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
    public Page<JsonResult> similaritySearchJson(float[] vector, Filter metadataFilter) {
        return mapAsPageJsonResult(similaritySearch(vector, metadataFilter));
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
    public Page<JsonResult> similaritySearchJson(float[] vector, String pagingState) {
        return mapAsPageJsonResult(similaritySearch(vector,  pagingState));
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
    public Page<JsonResult> similaritySearchJson(float[] vector, Filter metadataFilter, String pagingState) {
        return mapAsPageJsonResult(similaritySearch(vector, metadataFilter,  pagingState));
    }

    /**
     * Need the Json.
     *
     * @param vector
     *      curren vector
     * @param limit
     *      current limit
     * @return
     *      list of document
     */
    public List<JsonResult> similaritySearchJson(float[] vector, Integer limit) {
        return mapAsListJsonResult(similaritySearch(vector, limit));
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
    public List<JsonResult> similaritySearchJson(float[] vector, Filter metadataFilter, Integer limit) {
        return mapAsListJsonResult(similaritySearch(vector, metadataFilter, limit));
    }

    /**
     * Mapping method to have Json.
     *
     * @param res
     *      current result
     * @return
     *      list of document
     */
    private List<JsonResult> mapAsListJsonResult(List<Result<ObjectMap>> res) {
        return res.stream()
                .map(Result::toJsonResult)
                .collect(Collectors.toList());
    }

    /**
     * Mapping method to have Json.
     *
     * @param res
     *      current result
     * @return
     *      page of documents
     */
    private Page<JsonResult> mapAsPageJsonResult(Page<Result<ObjectMap>> res) {
        return new Page<>(res.getPageSize(), res.getPageState().orElse(null),
                mapAsListJsonResult(res.getResults())
        );
    }

}
