package io.stargate.sdk.json;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.sdk.api.ApiResponse;
import io.stargate.sdk.core.domain.Page;
import io.stargate.sdk.http.LoadBalancedHttpClient;
import io.stargate.sdk.http.ServiceHttp;
import io.stargate.sdk.http.domain.ApiResponseHttp;
import io.stargate.sdk.json.domain.JsonDocument;
import io.stargate.sdk.json.domain.JsonDocumentResult;
import io.stargate.sdk.json.domain.JsonFilter;
import io.stargate.sdk.json.domain.Query;
import io.stargate.sdk.json.exception.JsonApiException;
import io.stargate.sdk.json.utils.JsonApOperationUtils;
import io.stargate.sdk.utils.Assert;
import io.stargate.sdk.utils.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

import static io.stargate.sdk.json.utils.JsonApOperationUtils.buildRequestBody;
import static io.stargate.sdk.utils.AnsiUtils.green;
import static io.stargate.sdk.utils.AnsiUtils.yellow;

public class JsonDocumentsClient {

    /** Logger for our Client. */
    private static final Logger LOGGER = LoggerFactory.getLogger(JsonDocumentsClient.class);

    /**
     * Type reference for a list of JsonDocumentResult.
     */
    private static TypeReference<List<JsonDocumentResult>> DOCUMENT_LIST = new TypeReference<List<JsonDocumentResult>>(){};

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

    private Map<?,?> executeRequest(String stringBody) {
        LOGGER.debug("Request  " + yellow("{}"), stringBody);
        ApiResponseHttp res = stargateHttpClient.POST(collectionResource, stringBody);
        Map<?,?> body = JsonUtils.unmarshallBean(res.getBody(), Map.class);
        LOGGER.debug("Response " + yellow("{}"), res.getBody());
        JsonApOperationUtils.handleErrors(body);
        return body;
    }

    /**
     * Insert a new document.
     *
     * @return
     *      identifier for the document
     */
    public String insertOne(JsonDocument doc) {
        return insert(doc).get(0);
    }

    @SuppressWarnings("unchecked")
    public final <T> List<String> insert(JsonDocument... documents) {
        Objects.requireNonNull(documents, "documents");
        LOGGER.info("insert into {}/{}", green(namespace), green(collection));
        Map<?,?> body = executeRequest(buildRequestBody("insertMany", Map.of("documents", documents)));
        if (body.containsKey("status")) {
            Map<?, ?> status = (Map<?, ?>) body.get("status");
            if (status.containsKey("insertedIds")) {
                return ((ArrayList<String>) status.get("insertedIds"));
            }
        }
        throw new JsonApiException("No insertedIds found in the response");
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
        LOGGER.info("Counting {}/{}", green(namespace), green(collection));
        String stringBody = buildRequestBody("countDocuments", jsonFilter);
        LOGGER.info("Request  " + yellow("{}"), stringBody);
        ApiResponseHttp res = stargateHttpClient.POST(collectionResource, stringBody);
        Map<?,?> body = JsonUtils.unmarshallBean(res.getBody(), Map.class);
        LOGGER.info("Response " + yellow("{}"), res.getBody());
        JsonApOperationUtils.handleErrors(body);
        if (body.containsKey("status")) {
            Map<?, ?> status = (Map<?, ?>) body.get("status");
            if (status.containsKey("count")) {
                return (Integer) status.get("count");
            }
        }
        return 0;
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
        LOGGER.info("Query in {}/{}", green(namespace), green(collection));
        String stringBody = buildRequestBody("find", query);
        LOGGER.info("Request  " + yellow("{}"), stringBody);
        ApiResponseHttp res = stargateHttpClient.POST(collectionResource, stringBody);
        Map<?,?> body = JsonUtils.unmarshallBean(res.getBody(), Map.class);
        LOGGER.info("Response " + yellow("{}"), res.getBody());
        JsonApOperationUtils.handleErrors(body);
        Page<JsonDocumentResult> resultPage = null;
        if (body.containsKey("data")) {
            Map<?,?> result = (Map<?,?>) body.get("data");
            int pageSize = (query.getOptions().get("limit") != null)  ? (Integer) query.getOptions().get("limit") : -1;
            String nextPageState = (String) result.get("nextPageState");
            resultPage = new Page<>(pageSize, nextPageState);
            if (result.containsKey("documents")) {
                resultPage.setResult(new ObjectMapper().convertValue(result.get("documents"), DOCUMENT_LIST));
            }
        }
        return resultPage;
    }

}
