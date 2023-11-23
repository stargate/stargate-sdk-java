package io.stargate.sdk.http;

import io.stargate.sdk.api.ApiConstants;
import io.stargate.sdk.audit.ServiceCallObserver;
import io.stargate.sdk.exception.AlreadyExistException;
import io.stargate.sdk.exception.AuthenticationException;
import io.stargate.sdk.http.audit.ServiceHttpCallEvent;
import io.stargate.sdk.http.domain.ApiResponseHttp;
import io.stargate.sdk.loadbalancer.UnavailableResourceException;
import io.stargate.sdk.utils.CompletableFutures;
import com.evanlennick.retry4j.CallExecutorBuilder;
import com.evanlennick.retry4j.Status;
import com.evanlennick.retry4j.config.RetryConfig;
import com.evanlennick.retry4j.config.RetryConfigBuilder;
import org.apache.hc.client5.http.auth.StandardAuthScheme;
import org.apache.hc.client5.http.classic.methods.*;
import org.apache.hc.client5.http.config.RequestConfig;
import org.apache.hc.client5.http.cookie.StandardCookieSpec;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.CloseableHttpResponse;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManager;
import org.apache.hc.core5.http.ClassicHttpRequest;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.Method;
import org.apache.hc.core5.http.ParseException;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.http.io.entity.StringEntity;
import org.apache.hc.core5.util.TimeValue;
import org.apache.hc.core5.util.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * Wrapping the HttpClient and provide helpers
 * 
 * @author Cedrick LUNVEN (@clunven)
 */
public class RetryHttpClient implements ApiConstants {
    
    /** Logger for our Client. */
    private static final Logger LOGGER = LoggerFactory.getLogger(RetryHttpClient.class);
    
    /** Default settings in Request and Retry */
    public static final int DEFAULT_TIMEOUT_REQUEST   = 20;
    
    /** Default settings in Request and Retry */
    public static final int DEFAULT_TIMEOUT_CONNECT   = 20;
    
    /** Default settings in Request and Retry */
    public static final int DEFAULT_RETRY_COUNT       = 3;
    
    /** Default settings in Request and Retry */
    public static final Duration DEFAULT_RETRY_DELAY  = Duration.ofMillis(100);
    
    // -------------------------------------------
    // ----------------   Settings  --------------
    // -------------------------------------------
    
    /** Singleton pattern. */
    private static RetryHttpClient _instance = null;
    
    /** HttpComponent5. */
    protected CloseableHttpClient httpClient = null;
    
    /** Observers. */
    protected static Map<String, ServiceCallObserver<?,?,?>> apiInvocationsObserversMap = new ConcurrentHashMap<>();

    /** Default request configuration. */
    protected static RequestConfig requestConfig = RequestConfig.custom()
            .setCookieSpec(StandardCookieSpec.STRICT)
            .setExpectContinueEnabled(true)
            .setConnectionRequestTimeout(Timeout.ofSeconds(DEFAULT_TIMEOUT_REQUEST))
            .setResponseTimeout(Timeout.ofSeconds(DEFAULT_TIMEOUT_CONNECT))
            .setTargetPreferredAuthSchemes(Arrays.asList(StandardAuthScheme.NTLM, StandardAuthScheme.DIGEST))
            .build();

    /** Default retry configuration. */
    protected static RetryConfig retryConfig = new RetryConfigBuilder()
            //.retryOnSpecificExceptions(ConnectException.class, IOException.class)
            .retryOnAnyException()
            .withDelayBetweenTries(DEFAULT_RETRY_DELAY)
            .withExponentialBackoff()
            .withMaxNumberOfTries(DEFAULT_RETRY_COUNT)
            .build();

    /**
     * Update Retry configuration of the HTTPClient.
     *
     * @param conf
     *      retryConfiguration
     */
    public static void withRetryConfig(RetryConfig conf) {
        retryConfig= conf;
    }

    /**
     * Update RequestConfig configuration of the HTTPClient.
     *
     * @param conf
     *      RequestConfig
     */
    public static void withRequestConfig(RequestConfig conf) {
        requestConfig = conf;
    }

    /**
     * Register a new listener.
     *
     * @param name
     *      current name
     * @param listener
     *      current listener
     */
    public static void registerListener(String name, ServiceCallObserver<?,?,?> listener) {
        apiInvocationsObserversMap.put(name, listener);
    }
    
    // -------------------------------------------
    // ----------------- Singleton ---------------
    // -------------------------------------------
    
    /**
     * Hide default constructor
     */
    private RetryHttpClient() {}
    
    /**
     * Singleton Pattern.
     * 
     * @return
     *      singleton for the class
     */
    public static synchronized RetryHttpClient getInstance() {
        if (_instance == null) {
            _instance = new RetryHttpClient();
            final PoolingHttpClientConnectionManager connManager = new PoolingHttpClientConnectionManager();
            connManager.setValidateAfterInactivity(TimeValue.ofSeconds(10));
            connManager.setMaxTotal(100);
            connManager.setDefaultMaxPerRoute(10);
            _instance.httpClient = HttpClients.custom().setConnectionManager(connManager).build();
        }
        return _instance;
    }
    
    // -------------------------------------------
    // ---------- Working with HTTP --------------
    // -------------------------------------------
    
    /**
     * Helper to build the HTTP request.
     *
     * @param sHttp
     *      service http
     * @param url
     *      target url
     * @param token
     *      authentication token
     * @return
     *      http request
     */
    public ApiResponseHttp GET(ServiceHttp sHttp, String url, String token) {
        return executeHttp(sHttp, Method.GET, url, token, null, CONTENT_TYPE_JSON, false);
    }

    /**
     * Helper to build the HTTP request.
     *
     * @param sHttp
     *      service http
     * @param url
     *      target url
     * @param token
     *      authentication token
     * @return
     *      http request
     */
    public ApiResponseHttp HEAD(ServiceHttp sHttp, String url, String token) {
        return executeHttp(sHttp, Method.HEAD, url, token, null, CONTENT_TYPE_JSON, false);
    }
    
    /**
     * Helper to build the HTTP request.
     *
     * @param sHttp
     *      service http
     * @param url
     *      target url
     * @param token
     *      authentication token
     * @return
     *      http request
     */
    public ApiResponseHttp POST(ServiceHttp sHttp, String url, String token) {
        return executeHttp(sHttp, Method.POST, url, token, null, CONTENT_TYPE_JSON, true);
    }
    
    /**
     * Helper to build the HTTP request.
     *
     * @param sHttp
     *      service http
     * @param url
     *      target url
     * @param token
     *      authentication token
     * @param body
     *      request body     
     * @return
     *      http request
     */
    public ApiResponseHttp POST(ServiceHttp sHttp, String url, String token, String body) {
        return executeHttp(sHttp, Method.POST, url, token, body, CONTENT_TYPE_JSON, true);
    }
    
    /**
     * Helper to build the HTTP request.
     *
     * @param sHttp
     *      service http
     * @param url
     *      target url
     * @param token
     *      authentication token
     * @param body
     *      request body     
     * @return
     *      http request
     */
    public ApiResponseHttp POST_GRAPHQL(ServiceHttp sHttp,  String url, String token, String body) {
        return executeHttp(sHttp, Method.POST, url, token, body, CONTENT_TYPE_GRAPHQL, true);
    }
    
    /**
     * Helper to build the HTTP request.
     *
     * @param sHttp
     *      service http
     * @param url
     *      target url
     * @param token
     *      authentication token
     * @return
     *      http request
     */
    public ApiResponseHttp DELETE(ServiceHttp sHttp,  String url, String token) {
        return executeHttp(sHttp, Method.DELETE, url, token, null, CONTENT_TYPE_JSON, true);
    }
    
    /**
     * Helper to build the HTTP request.
     *
     * @param sHttp
     *      service http
     * @param url
     *      target url
     * @param token
     *      authentication token
     * @param body
     *      request body     
     * @return
     *      http request
     */
    public ApiResponseHttp PUT(ServiceHttp sHttp,  String url, String token, String body) {
        return executeHttp(sHttp, Method.PUT, url, token, body, CONTENT_TYPE_JSON, false);
    }
    
    /**
     * Helper to build the HTTP request.
     *
     * @param sHttp
     *      service http
     * @param url
     *      target url
     * @param token
     *      authentication token
     * @param body
     *      request body     
     * @return
     *      http request
     */
    public ApiResponseHttp PATCH(ServiceHttp sHttp, String url, String token, String body) {
        return executeHttp(sHttp, Method.PATCH, url, token, body, CONTENT_TYPE_JSON, true);
    }
    
    /**
     * Main Method executing HTTP Request.
     *
     * @param sHttp
     *      service http
     * @param method
     *      http method
     * @param url
     *      url
     * @param token
     *      authentication token
     * @param contentType
     *      request content type
     * @param reqBody
     *      request body
     * @param mandatory
     *      allow 404 errors
     * @return
     *      basic request
     */
    public ApiResponseHttp executeHttp(ServiceHttp sHttp, final Method method, final String url, final String token, String reqBody, String contentType, boolean mandatory) {
        return executeHttp(sHttp, buildRequest(method, url, token, reqBody, contentType), mandatory);
    }

    /**
     * Execute a request coming from elsewhere.
     *
     * @param sHttp
     *      service http
     * @param req
     *      current request
     * @param mandatory
     *      mandatory
     * @return
     *      api response
     */
    public ApiResponseHttp executeHttp(ServiceHttp sHttp, HttpUriRequestBase req, boolean mandatory) {
        // Initializing the invocation event
        ServiceHttpCallEvent event = new ServiceHttpCallEvent(sHttp, req);
        // Invoking the expected endpoint
        Status<CloseableHttpResponse> status = executeWithRetries(req);
        try {
            // Parsing result as expected bean
            ApiResponseHttp res = mapResponse(status, event);
            // Error management
            if (HttpURLConnection.HTTP_NOT_FOUND == res.getCode() && !mandatory) {
                return res;
            }
            if (res.getCode() >= 300) {
              LOGGER.error("Error for request [{}], url={}, method={}, code={}, body={}", 
                      event.getRequestId(),
                      req.getUri().toString(), req.getMethod(),
                      res.getCode(), res.getBody());
              processErrors(res, mandatory);
              logHttpError(res);
            }
            return res;
        } catch (UnavailableResourceException e) {
            event.setErrorClass(e.getClass().getName());
            event.setErrorMessage(e.getMessage());
            throw e;
        } catch (IllegalArgumentException e) {
            event.setErrorClass(e.getClass().getName());
            event.setErrorMessage(e.getMessage());
            throw e;
        } catch (Exception e) {
            event.setErrorClass(e.getClass().getName());
            event.setErrorMessage(e.getMessage());
            throw new RuntimeException("Error in HTTP Request", e);
        } finally {
            CompletableFuture.runAsync(()-> notifyAsync(listener->listener.onCall(event)));
        }
    }
    
    
    
    /**
     * Mapping HTTP Response to framework HTTP BEAN.
     *
     * @param status
     *      current result of the retries
     * @param event
     *      event to be sent
     * @return
     *      bean populated
     * @throws ParseException
     *      error in parsing
     * @throws IOException
     *      error in accessing payload
     */
    private ApiResponseHttp mapResponse(Status<CloseableHttpResponse> status, ServiceHttpCallEvent event)
    throws ParseException, IOException {
        ApiResponseHttp res;
        event.setTotalTries(status.getTotalTries());
        event.setLastException(status.getLastExceptionThatCausedRetry());
        event.setResponseElapsedTime(status.getTotalElapsedDuration().toMillis());
        try (CloseableHttpResponse response = status.getResult()) {
            event.setResponseTimestamp(status.getEndTime());
            if (response == null) {
                event.setHttpResponseCode(HttpURLConnection.HTTP_UNAVAILABLE);
                res = new ApiResponseHttp("Response is empty, please check url", 
                        HttpURLConnection.HTTP_UNAVAILABLE, null);
            } else {
                event.setHttpResponseCode(response.getCode());
                Map<String, String > headers = new HashMap<>();
                Arrays.stream(response.getHeaders()).forEach(h -> headers.put(h.getName(), h.getValue()));
                event.setHttpResponseHeaders(headers);
    
                // Parse body if present
                String body = null;
                if (null != response.getEntity()) {
                     body = EntityUtils.toString(response.getEntity());
                     EntityUtils.consume(response.getEntity());
                }
                event.setHttpResponseBody(body);
            
                // Mapping response
                res = new ApiResponseHttp(body, response.getCode(), headers);
            }
        }
        return res;
    }
    
    /**
     * Asynchronously send calls to listener for tracing.
     *
     * @param lambda
     *      operations to execute
     * @return
     *      void
     */
    private CompletionStage<Void> notifyAsync(Consumer<ServiceCallObserver> lambda) {
        return CompletableFutures.allDone(apiInvocationsObserversMap.values().stream()
                .map(l -> CompletableFuture.runAsync(() -> lambda.accept(l)))
                .collect(Collectors.toList()));
    }

    /**
     * Initialize an HTTP request against Stargate.
     * 
     * @param method
     *      http Method
     * @param url
     *      target URL
     * @param token
     *      current token
     * @return
     *      default http with header
     */
    private HttpUriRequestBase buildRequest(final Method method, final String url, final String token, String body, String contentType) {
        HttpUriRequestBase req;
        switch(method) {
            case GET:    req = new HttpGet(url);    break;
            case POST:   req = new HttpPost(url);   break;
            case PUT:    req = new HttpPut(url);    break;
            case DELETE: req = new HttpDelete(url); break;
            case PATCH:  req = new HttpPatch(url);  break;
            case HEAD:   req = new HttpHead(url);   break;
            case TRACE:  req = new HttpTrace(url);  break;
            case OPTIONS:
            case CONNECT:
            default:throw new IllegalArgumentException("Invalid HTTP Method");
        }
        req.addHeader(HEADER_CONTENT_TYPE, contentType);
        req.addHeader(HEADER_ACCEPT, CONTENT_TYPE_JSON);
        req.addHeader(HEADER_USER_AGENT, REQUEST_WITH);
        req.addHeader(HEADER_REQUEST_ID, UUID.randomUUID().toString());
        req.addHeader(HEADER_REQUESTED_WITH, REQUEST_WITH);
        req.addHeader(HEADER_CASSANDRA, token);
        req.addHeader(HEADER_AUTHORIZATION, "Bearer " + token);
        req.setConfig(requestConfig);
        if (null != body) {
            // If you don't set a Charset the client will use ISO-8859-1
            // preventing the use of UNICODE characters, and also the server assumes UTF-8
            // that lead to decoding issues.
            req.setEntity(new StringEntity(body, ContentType.TEXT_PLAIN.withCharset(StandardCharsets.UTF_8)));
        }
        return req;
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
    private Status<CloseableHttpResponse> executeWithRetries(ClassicHttpRequest req) {
        Callable<CloseableHttpResponse> executeRequest = () -> {
            return httpClient.execute(req);
        };
        return new CallExecutorBuilder<String>()
                .config(retryConfig)
                .onSuccessListener(s -> {
                    CompletableFuture.runAsync(()-> notifyAsync(listener->listener.onSuccess(s)));
                })
                .onCompletionListener(s -> {
                    CompletableFuture.runAsync(()-> notifyAsync(listener->listener.onCompletion(s)));
                })
                .onFailureListener(s -> {
                    LOGGER.error("Calls failed after {} retries", s.getTotalTries());
                    CompletableFuture.runAsync(()-> notifyAsync(listener->listener.onFailure(s)));
                })
                .afterFailedTryListener(s -> {
                    LOGGER.error("Failure on attempt {}/{} ", s.getTotalTries(), retryConfig.getMaxNumberOfTries());
                    try {
                        LOGGER.error("Failed request {} on {}", req.getMethod() , req.getUri() );
                        LOGGER.error("+ Exception was ", s.getLastExceptionThatCausedRetry());
                    } catch (URISyntaxException e) {
                        LOGGER.error("Cannot display URI ", e);
                    }
                    CompletableFuture.runAsync(()-> notifyAsync(listener->listener.onFailedTry(s)));
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
    
    private void logHttpError(ApiResponseHttp res) {
        LOGGER.error("An HTTP Error occurred. The HTTP CODE Return is {}", res.getCode());
    }

    /**
     * Getter accessor for attribute 'requestConfig'.
     *
     * @return
     *       current value of 'requestConfig'
     */
    public static RequestConfig getRequestConfig() {
        return requestConfig;
    }

    /**
     * Getter accessor for attribute 'retryConfig'.
     *
     * @return
     *       current value of 'retryConfig'
     */
    public static RetryConfig getRetryConfig() {
        return retryConfig;
    }

}
