package io.stargate.sdk.json;

import io.stargate.sdk.http.LoadBalancedHttpClient;
import io.stargate.sdk.http.ServiceHttp;
import io.stargate.sdk.http.domain.ApiResponseHttp;
import io.stargate.sdk.json.domain.Query;
import io.stargate.sdk.json.utils.JsonApOperationUtils;
import io.stargate.sdk.utils.Assert;
import io.stargate.sdk.utils.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.function.Function;

import static io.stargate.sdk.utils.AnsiUtils.green;

public class JsonDocumentsClient {

    /** Logger for our Client. */
    private static final Logger LOGGER = LoggerFactory.getLogger(JsonNamespacesClient.class);

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
     * Count Document request.
     *
     * @return
     *      number of document.
     */
    public int countDocuments(Query query) {
        Query q = Query.builder()
             .where("location")
                .isEqualsTo("london")
             .and("race.competitor")
                .isEqualsTo(100)
             .build();
        String stringBody = JsonApOperationUtils.buildRequestBody("countDocuments");
        ApiResponseHttp res = stargateHttpClient.POST(collectionResource, q.getWhere().orElse(null));
        Map<?,?> body = JsonUtils.unmarshallBean(res.getBody(), Map.class);
        JsonApOperationUtils.handleErrors(body);
        if (body.containsKey("status")) {
            Map<?, ?> status = (Map<?, ?>) body.get("status");
            if (status.containsKey("count")) {
                return (Integer) status.get("count");
            }
        }
        return 0;
    }

}
