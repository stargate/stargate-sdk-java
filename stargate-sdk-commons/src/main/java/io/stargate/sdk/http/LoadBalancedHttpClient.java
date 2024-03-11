package io.stargate.sdk.http;

import io.stargate.sdk.ManagedServiceDeployment;
import io.stargate.sdk.ServiceDeployment;
import io.stargate.sdk.api.ApiConstants;
import io.stargate.sdk.http.domain.ApiResponseHttp;
import io.stargate.sdk.loadbalancer.LoadBalancedResource;
import io.stargate.sdk.loadbalancer.NoneResourceAvailableException;
import io.stargate.sdk.loadbalancer.UnavailableResourceException;
import io.stargate.sdk.utils.Assert;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Function;

/**
 * Rest API is an Http Service of Stargate
 */
@Slf4j
@Getter
public class LoadBalancedHttpClient implements ApiConstants {

    /** Logger for our Client. */
    private static final Logger LOGGER = LoggerFactory.getLogger(LoadBalancedHttpClient.class);

    /** Hold the configuration of the cluster with list of dc and service instances. */
    private final ManagedServiceDeployment<ServiceHttp> deployment;

    /** Reference an http client with retry. */
    private final RetryHttpClient retryHttpClient;

    /**
     * Complete configuration.
     *
     * @param conf
     *      configuration
     */
    public LoadBalancedHttpClient(ServiceDeployment<ServiceHttp> conf) {
        this(conf, HttpClientOptions.builder().build());
    }

    /**
     * Complete configuration.
     * @param conf
     *      configuration
     */
    public LoadBalancedHttpClient(ServiceDeployment<ServiceHttp> conf, HttpClientOptions options) {
        Assert.notNull(conf, "deployment");;
        Assert.notNull(options, "http client options");;
        this.deployment      = new ManagedServiceDeployment<>(conf);
        this.retryHttpClient = new RetryHttpClient(options);
    }

    /**
     * Execute a GET HTTP Call on a StargateNode
     *
     * @param mapper
     *      build the target URL
     * @return
     *      http response
     */
    public ApiResponseHttp GET(Function<ServiceHttp, String> mapper) {
        return GET(mapper, null);
    }

    /**
     * Syntax sugar.
     *
     * @param mapper
     *      mapper for the URL
     * @param suffix
     *      suffix for the URL
     * @return
     *      http response
     */
    public ApiResponseHttp GET(Function<ServiceHttp, String> mapper, String suffix) {
        return http(mapper, "GET", null, suffix, CONTENT_TYPE_JSON, false);
    }

    /**
     * Syntax sugar for a HEAD.
     *
     * @param mapper
     *       mapper for the URL
     * @return
     *      http response
     */
    public ApiResponseHttp HEAD(Function<ServiceHttp, String> mapper) {
        return http(mapper, "PATCH", null, null, CONTENT_TYPE_JSON, false);
    }

    /**
     * Syntaxic sugar for a HEAD.
     *
     * @param mapper
     *       mapper for the URL
     * @return
     *      http response
     */
    public ApiResponseHttp POST(Function<ServiceHttp, String> mapper) {
        return POST(mapper,  null);
    }

    /**
     * Syntaxic sugar for a HEAD.
     *
     * @param mapper
     *       mapper for the URL
     * @param body
     *      provide a request body
     * @return
     *      http response
     */
    public ApiResponseHttp POST(Function<ServiceHttp, String> mapper, String body) {
        return http(mapper, "POST", body, null, CONTENT_TYPE_JSON, true);
    }

    /**
     * Syntaxic sugar for a HEAD.
     *
     * @param mapper
     *       mapper for the URL
     * @param body
     *      provide a request body
     * @return
     *      http response
     */
    public ApiResponseHttp POST_GRAPHQL(Function<ServiceHttp, String> mapper, String body) {
        return http(mapper, "POST", body, null, CONTENT_TYPE_GRAPHQL, true);
    }

    /**
     * Syntax sugar.
     *
     * @param mapper
     *       mapper for the URL
     * @param body
     *      provide a request body
     * @param suffix
     *      URL suffix
     * @return
     *      http response
     */
    public ApiResponseHttp POST(Function<ServiceHttp, String> mapper, String body, String suffix) {
        return http(mapper, "POST", body, suffix, CONTENT_TYPE_JSON, true);
    }

    /**
     * Syntax sugar.
     *
     * @param mapper
     *      mapper for the URL
     * @return
     *       http response
     */
    public ApiResponseHttp DELETE(Function<ServiceHttp, String> mapper) {
        return http(mapper, "DELETE", null, null, CONTENT_TYPE_JSON, true);
    }

    /**
     * Syntax sugar.
     *
     * @param mapper
     *      mapper for the URL
     * @param suffix
     *      URL suffix
     * @return
     *       http response
     */
    public ApiResponseHttp DELETE(Function<ServiceHttp, String> mapper, String suffix) {
        return http(mapper, "DELETE", null, suffix, CONTENT_TYPE_JSON, true);
    }

    /**
     * Syntax sugar.
     *
     * @param mapper
     *       mapper for the URL
     * @param body
     *      provide a request body
     * @return
     *      http response
     */
    public ApiResponseHttp PUT(Function<ServiceHttp, String> mapper, String body) {
        return http(mapper, "PUT", body, null, CONTENT_TYPE_JSON, false);
    }

    /**
     * Syntax sugar.
     *
     * @param mapper
     *       mapper for the URL
     * @param body
     *      provide a request body
     * @param suffix
     *      URL suffix
     * @return
     *      http response
     */
    public ApiResponseHttp PUT(Function<ServiceHttp, String> mapper, String body, String suffix) {
        return http(mapper, "PUT", body, suffix, CONTENT_TYPE_JSON, false);
    }

    /**
     * Syntaxic sugar.
     *
     * @param mapper
     *       mapper for the URL
     * @param body
     *      provide a request body
     * @return
     *      http response
     */
    public ApiResponseHttp PATCH(Function<ServiceHttp, String> mapper, String body) {
        return http(mapper, "PATCH", body, null, CONTENT_TYPE_JSON, true);
    }

    /**
     * Syntax sugar.
     *
     * @param mapper
     *       mapper for the URL
     * @param body
     *      provide a request body
     * @param suffix
     *      URL suffix
     * @return
     *      http response
     */
    public ApiResponseHttp PATCH(Function<ServiceHttp, String> mapper, String body, String suffix) {
        return http(mapper, "PATCH", body, suffix, CONTENT_TYPE_JSON, true);
    }

    /**
     * Generic Method to build and execute http request with retries, load balancing and failover.
     *
     * @param mapper
     *      building the request from a node
     * @param method
     *      http method used
     * @param body
     *      request body (optional)
     * @param suffix
     *      URL suffix
     * @param mandatory
     *      handling 404 error code, could raise exception or not
     * @return
     *      http response
     */
    private ApiResponseHttp http(Function<ServiceHttp, String> mapper,
                                 final String method, String body,
                                 String suffix, String contentType,
                                 boolean mandatory) {
        Assert.notNull(mapper, "function mapper");
        Assert.hasLength(method, "method");
        LoadBalancedResource<ServiceHttp> lb = null;
        while (true) {
            try {
                // Get an available node from LB
                lb = deployment.lookupStargateNode();
                // Build Parameters
                String targetEndPoint = mapper.apply(lb.getResource());
                if (null != suffix) targetEndPoint+= suffix;
                // Invoke request
                return retryHttpClient.executeHttp(method, targetEndPoint, deployment.lookupToken(), body, contentType, mandatory);
            } catch(UnavailableResourceException rex) {
                LOGGER.warn("A stargate node is down [{}], falling back to another node...", lb.getResource().getId());
                try {
                    deployment.failOverStargateNode(lb, rex);
                } catch (NoneResourceAvailableException nex) {
                    LOGGER.warn("No node availables is localDc [{}], falling back to another DC if available ...",
                            deployment.getLocalDatacenterClient().getDatacenterName());
                    deployment.failOverDatacenter();
                }
            } catch(NoneResourceAvailableException nex) {
                LOGGER.warn("No node availables is DataCenter [{}], falling back to another DC if available ...",
                        deployment.getLocalDatacenterClient().getDatacenterName());
                deployment.failOverDatacenter();
            }
        }
    }

}

