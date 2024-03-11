package io.stargate.sdk.http;

import com.evanlennick.retry4j.CallExecutorBuilder;
import com.evanlennick.retry4j.Status;
import com.evanlennick.retry4j.config.RetryConfig;
import com.evanlennick.retry4j.config.RetryConfigBuilder;
import io.stargate.sdk.api.ApiConstants;
import io.stargate.sdk.exception.AlreadyExistException;
import io.stargate.sdk.exception.AuthenticationException;
import io.stargate.sdk.http.domain.ApiResponseHttp;
import io.stargate.sdk.loadbalancer.UnavailableResourceException;
import lombok.extern.slf4j.Slf4j;

import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.ProxySelector;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

/**
 * Http Client using JDK11 client with a retry mechanism.
 */
@Slf4j
public class RetryHttpClient implements ApiConstants {

    // -------------------------------------------
    // ----------------   Settings  --------------
    // -------------------------------------------

    /** JDK11 Http client. */
    protected HttpClient httpClient;

    /** Default retry configuration. */
    protected RetryConfig retryConfig;

    /** hold reference to set configuration in the requests. */
    protected HttpClientOptions httpClientOptions;

    /** Default settings in Request and Retry */
    public LinkedHashMap<String, String> userAgents = new LinkedHashMap<>();

    /**
     * Hide default constructor
     */
    public RetryHttpClient() {
        this(HttpClientOptions.builder().build());
    }

    /**
     * Initialize the instance with all items
     *
     * @param config
     *      configuration of the HTTP CLIENT.
     */
    public RetryHttpClient(HttpClientOptions config) {
        this.httpClientOptions = config;

        HttpClient.Builder httpClientBuilder = HttpClient.newBuilder();
        httpClientBuilder.version(config.getHttpVersion());
        httpClientBuilder.followRedirects(config.getHttpRedirect());
        httpClientBuilder.connectTimeout(Duration.ofSeconds(config.connectionRequestTimeoutInSeconds));
        if (config.getProxy() != null) {
            httpClientBuilder.proxy(ProxySelector.of(new InetSocketAddress(
                    config.getProxy().getHostname(),
                    config.getProxy().getPort())));
        }
        httpClient = httpClientBuilder.build();

        retryConfig = new RetryConfigBuilder()
                .retryOnAnyException()
                .withDelayBetweenTries(Duration.ofMillis(config.getRetryDelay()))
                .withMaxNumberOfTries(config.getRetryCount())
                .withExponentialBackoff()
                .build();

        pushUserAgent(config.userAgentCallerName, config.userAgentCallerVersion);
    }

    /**
     * Add an item to the user agent chain.
     *
     * @param component
     *      component
     * @param version
     *      version number
     */
    public void pushUserAgent(String component, String version) {
        if (!userAgents.containsKey(component)) {
            userAgents.put(component, version);
        }
    }

    /**
     * Give access to the user agent header.
     *
     * @return
     *      user agent header
     */
    public String getUserAgentHeader() {
        if (userAgents.isEmpty()) {
            userAgents.put(ApiConstants.REQUEST_WITH, ApiConstants.class.getPackage().getImplementationVersion());
        }
        List<Map.Entry<String, String>> entryList = new ArrayList<>(userAgents.entrySet());
        StringBuilder sb = new StringBuilder();
        for (int i = entryList.size() - 1; i >= 0; i--) {
            Map.Entry<String, String> entry = entryList.get(i);
            sb.append(entry.getKey()).append("/").append(entry.getValue());
            if (i > 0) { // Add a space between entries, but not after the last entry
                sb.append(" ");
            }
        }
        return sb.toString();
    }

    // -------------------------------------------
    // ---------- Working with HTTP --------------
    // -------------------------------------------

    /**
     * Helper to build the HTTP request.
     *
     * @param url
     *      target url
     * @param token
     *      authentication token
     * @return
     *      http request
     */
    public ApiResponseHttp GET(String url, String token) {
        return executeHttp("GET", url, token, null, CONTENT_TYPE_JSON, false);
    }

    /**
     * Helper to build the HTTP request.
     *
     * @param url
     *      target url
     * @param token
     *      authentication token
     * @return
     *      http request
     */
    public ApiResponseHttp HEAD(String url, String token) {
        return executeHttp("HEAD", url, token, null, CONTENT_TYPE_JSON, false);
    }

    /**
     * Helper to build the HTTP request.
     *
     * @param url
     *      target url
     * @param token
     *      authentication token
     * @return
     *      http request
     */
    public ApiResponseHttp POST(String url, String token) {
        return executeHttp("POST", url, token, null, CONTENT_TYPE_JSON, true);
    }

    /**
     * Helper to build the HTTP request.
     *
     * @param url
     *      target url
     * @param token
     *      authentication token
     * @param body
     *      request body
     * @return
     *      http request
     */
    public ApiResponseHttp POST(String url, String token, String body) {
        return executeHttp("POST", url, token, body, CONTENT_TYPE_JSON, true);
    }

    /**
     * Helper to build the HTTP request.
     *
     * @param url
     *      target url
     * @param token
     *      authentication token
     * @param body
     *      request body
     * @return
     *      http request
     */
    public ApiResponseHttp POST_GRAPHQL(String url, String token, String body) {
        return executeHttp("POST", url, token, body, CONTENT_TYPE_GRAPHQL, true);
    }

    /**
     * Helper to build the HTTP request.
     *
     * @param url
     *      target url
     * @param token
     *      authentication token
     * @return
     *      http request
     */
    public ApiResponseHttp DELETE(String url, String token) {
        return executeHttp("DELETE", url, token, null, CONTENT_TYPE_JSON, true);
    }

    /**
     * Helper to build the HTTP request.
     *
     * @param url
     *      target url
     * @param token
     *      authentication token
     * @param body
     *      request body
     * @return
     *      http request
     */
    public ApiResponseHttp PUT(String url, String token, String body) {
        return executeHttp("PUT", url, token, body, CONTENT_TYPE_JSON, false);
    }

    /**
     * Helper to build the HTTP request.
     *
     * @param url
     *      target url
     * @param token
     *      authentication token
     * @param body
     *      request body
     * @return
     *      http request
     */
    public ApiResponseHttp PATCH(String url, String token, String body) {
        return executeHttp("PATCH", url, token, body, CONTENT_TYPE_JSON, true);
    }

    private HttpRequest builtHttpRequest(final String method,
                                         final String url,
                                         final String token,
                                         String body,
                                         String contentType) {
        try {
            return HttpRequest.newBuilder()
                .uri(new URI(url))
                .header(HEADER_CONTENT_TYPE, contentType)
                .header(HEADER_ACCEPT, CONTENT_TYPE_JSON)
                .header(HEADER_USER_AGENT, getUserAgentHeader())
                .header(HEADER_REQUESTED_WITH, getUserAgentHeader())
                .header(HEADER_REQUEST_ID, UUID.randomUUID().toString())
                .header(HEADER_CASSANDRA, token)
                .header(HEADER_AUTHORIZATION, "Bearer " + token)
                .timeout(Duration.ofSeconds(httpClientOptions.responseTimeoutInSeconds))
                .method(method, (body==null) ?
                        HttpRequest.BodyPublishers.noBody() :
                        HttpRequest.BodyPublishers.ofString(body))
                .build();
        } catch(URISyntaxException e) {
            throw new IllegalArgumentException("Invalid URL '" + url + "'", e);
        }
    }

    private ApiResponseHttp parseHttpResponse(HttpResponse<String> response, boolean mandatory) {
        try {
            // Parsing result as expected bean
            ApiResponseHttp res;
            if (response == null) {
                return new ApiResponseHttp("Response is empty, please check url",
                        HttpURLConnection.HTTP_UNAVAILABLE, null);
            }
            res = new ApiResponseHttp(response.body(), response.statusCode(),
                    response.headers().map().entrySet()
                            .stream()
                            .collect(Collectors.toMap(Map.Entry::getKey,
                                    entry -> entry.getValue().toString()))
            );
            // Error management
            if (HttpURLConnection.HTTP_NOT_FOUND == res.getCode() && !mandatory) {
                return res;
            }
            if (res.getCode() >= 300) {
                log.error("Error for request url={}, method={}, code={}, body={}",
                        response.request().uri().toString(), response.request().method(),
                        res.getCode(), res.getBody());
                processErrors(res, mandatory);
            }
            return res;
        } catch (UnavailableResourceException e) {
            log.error("Cannot find resource to execute query",e);
            throw e;
        } catch (IllegalArgumentException e) {
            log.error("Invalid argument", e);
            throw e;
        } catch (Exception e) {
            throw new RuntimeException("Error in HTTP Request", e);
        }
    }

    /**
     * Main Method executing HTTP Request.
     *
     * @param method
     *      http method
     * @param url
     *      url
     * @param token
     *      authentication token
     * @param contentType
     *      request content type
     * @param body
     *      request body
     * @param mandatory
     *      allow 404 errors
     * @return
     *      basic request
     */
    public ApiResponseHttp executeHttp(final String method,
                                       final String url,
                                       final String token,
                                       String body,
                                       String contentType, boolean mandatory) {
            // Parse request
            HttpRequest httpRequest = builtHttpRequest(method, url, token, body, contentType);

            // Invoking the expected endpoint
            Status<HttpResponse<String>> status = executeHttpRequest(httpRequest);

            if (status.wasSuccessful()) {
                return parseHttpResponse(status.getResult(), mandatory);
            }
            throw new RuntimeException(status.getLastExceptionThatCausedRetry());
    }

    public ApiResponseHttp executeHttp( HttpRequest httpRequest , boolean mandatory) {
        // Invoking the expected endpoint
        Status<HttpResponse<String>> status = executeHttpRequest(httpRequest);

        if (status.wasSuccessful()) {
            return parseHttpResponse(status.getResult(), mandatory);
        }
        throw new RuntimeException(status.getLastExceptionThatCausedRetry());
    }

    /**
     * Implementing retries.
     *
     * @param req
     *      current request
     * @return
     *      the closeable response
     */
    @SuppressWarnings("unchecked")
    private Status<HttpResponse<String>> executeHttpRequest(HttpRequest req) {
        Callable<HttpResponse<String>> executeRequest = () ->
                httpClient.send(req, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
        return new CallExecutorBuilder<String>()
                .config(retryConfig)
                .onFailureListener(s -> log.error("Calls failed after {} retries", s.getTotalTries()))
                .afterFailedTryListener(s -> {
                    log.error("Failure on attempt {}/{} ", s.getTotalTries(), retryConfig.getMaxNumberOfTries());
                    log.error("Failed request {} on {}", req.method() , req.uri().toString() );
                    log.error("+ Exception was ", s.getLastExceptionThatCausedRetry());
                })
                .build()
                .execute(executeRequest);
    }

    /**
     * Process ERRORS.Anything above code 300 can be marked as an error Still something
     * 404 is expected and should not result in throwing exception (=not find)
     * @param res HttpResponse
     */
    private void processErrors(ApiResponseHttp res, boolean mandatory) {
        switch(res.getCode()) {
            // 400
            case HttpURLConnection.HTTP_BAD_REQUEST:
                throw new IllegalArgumentException("Error Code=" + res.getCode() +
                        " (HTTP_BAD_REQUEST) Invalid Parameters: "
                        + res.getBody());
                // 401
            case HttpURLConnection.HTTP_UNAUTHORIZED:
                throw new AuthenticationException("Error Code=" + res.getCode() +
                        ", (HTTP_UNAUTHORIZED) Invalid Credentials Check your token: " +
                        res.getBody());
                // 403
            case HttpURLConnection.HTTP_FORBIDDEN:
                throw new AuthenticationException("Error Code=" + res.getCode() +
                        ", (HTTP_FORBIDDEN) Invalid permissions, check your token: " +
                        res.getBody());
                // 404
            case HttpURLConnection.HTTP_NOT_FOUND:
                if (mandatory) {
                    throw new IllegalArgumentException("Error Code=" + res.getCode() +
                            "(HTTP_NOT_FOUND) Object not found:  "
                            + res.getBody());
                }
                break;
            // 409
            case HttpURLConnection.HTTP_CONFLICT:
                throw new AlreadyExistException("Error Code=" + res.getCode() +
                        ", (HTTP_CONFLICT) Object may already exist with same identifiers: " +
                        res.getBody());
            case 422:
                throw new IllegalArgumentException("Error Code=" + res.getCode() +
                        "(422) Invalid information provided to create DB: "
                        + res.getBody());
            default:
                if (res.getCode() == HttpURLConnection.HTTP_UNAVAILABLE) {
                    throw new UnavailableResourceException(res.getBody() + " (http:" + res.getCode() + ")");
                }
                throw new RuntimeException(res.getBody() + " (http:" + res.getCode() + ")");
        }
    }


}
