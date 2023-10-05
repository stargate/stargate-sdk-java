package io.stargate.sdk.json;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.sdk.core.domain.Page;
import io.stargate.sdk.http.LoadBalancedHttpClient;
import io.stargate.sdk.http.ServiceHttp;
import io.stargate.sdk.http.domain.ApiResponseHttp;
import io.stargate.sdk.json.domain.JsonApiData;
import io.stargate.sdk.json.domain.JsonApiResponse;
import io.stargate.sdk.json.domain.JsonDocument;
import io.stargate.sdk.json.domain.JsonDocumentResult;
import io.stargate.sdk.json.domain.JsonFilter;
import io.stargate.sdk.json.domain.Query;
import io.stargate.sdk.json.exception.JsonApiException;
import io.stargate.sdk.json.utils.JsonApiClientUtils;
import io.stargate.sdk.utils.Assert;
import io.stargate.sdk.utils.JsonUtils;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Stream;

import static io.stargate.sdk.json.utils.JsonApiClientUtils.buildRequestBody;
import static io.stargate.sdk.json.utils.JsonApiClientUtils.executeOperation;
import static io.stargate.sdk.utils.AnsiUtils.green;
import static io.stargate.sdk.utils.AnsiUtils.yellow;


@Slf4j
public class JsonDocumentsClient {

    /**
     * Type reference for a list of JsonDocumentResult.
     */
    private static final TypeReference<List<JsonDocumentResult>> DOCUMENT_LIST =
            new TypeReference<List<JsonDocumentResult>>(){};

    /** Get Topology of the nodes. */
    protected final LoadBalancedHttpClient stargateHttpClient;

    /** Collection. */
    private String namespace;

    private String collection;

    /**
     * /v1/{namespace}
     */
    public Function<ServiceHttp, String> collectionResource = (node) ->
            StargateJsonApiClient.rootResource.apply(node) + "/" + namespace + "/" + collection;

    /**
     * Full constructor.
     *
     * @param httpClient client http
     * @param namespace namespace identifier
     * @param collection collection identifier
     */
    public JsonDocumentsClient(LoadBalancedHttpClient httpClient, String namespace, String collection) {
        this.namespace          = namespace;
        this.collection         = collection;
        this.stargateHttpClient = httpClient;
        Assert.notNull(collection, "Collection");
        Assert.notNull(namespace, "namespace");
    }

    /**
     * Insert a new document.
     *
     * @return
     *      identifier for the document
     */
    public String insertOne(JsonDocument doc) {
        return insert(doc).findFirst().orElse(null);
    }

    /**
     * Insert multiple Objects
     * @param documents
     *      list of documents
     * @return
     *      list of ids
     */
    public final Stream<String> insert(JsonDocument... documents) {
        Objects.requireNonNull(documents, "documents");
        log.debug("insert into {}/{}", green(namespace), green(collection));
        return execute("insertMany", Map.of("documents", documents)).getStatusKeyAsStream("insertedIds");
    }

    /**
     * Count Document request.
     *
     * @return
     *      number of document.
     */
    public int countDocuments() {
        return countDocuments(null);
    }

    /**
     * Count Document request.
     *
     * @return
     *      number of document.
     */
    public int countDocuments(JsonFilter jsonFilter) {
        log.debug("Counting {}/{}", green(namespace), green(collection));
        return execute("countDocuments", jsonFilter).getStatusKeyAsInt("count");
    }

    /**
     * Find documents matching the query.
     *
     * @param query
     *      current query
     * @return
     *      page of results
     */
    public Page<JsonDocumentResult> find(Query query) {
        log.debug("Query in {}/{}", green(namespace), green(collection));
        JsonApiData apiData = execute("find", query).getData();
        int pageSize = (query.getOptions().get("limit") != null) ? (Integer) query.getOptions().get("limit") : -1;
        Page<JsonDocumentResult> resultPage = new Page<>(pageSize, apiData.getNextPageState());
        resultPage.setResult(apiData.getDocuments());
        return resultPage;
    }

    /**
     * Syntax sugar.
     *
     * @param operation
     *      operation to run
     * @param payload
     *      payload returned
     */
    private JsonApiResponse execute(String operation, Object payload) {
        return executeOperation(stargateHttpClient, collectionResource, operation, payload);
    }

}
