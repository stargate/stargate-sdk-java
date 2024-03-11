package io.stargate.sdk.auth;

import io.stargate.sdk.api.ApiConstants;
import io.stargate.sdk.http.RetryHttpClient;
import io.stargate.sdk.http.domain.ApiResponseHttp;
import io.stargate.sdk.loadbalancer.Loadbalancer;
import io.stargate.sdk.utils.Assert;
import io.stargate.sdk.utils.JsonUtils;

import java.net.URI;
import java.net.http.HttpRequest;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Using the authentication endpoint you should be able tp...
 *
 * @author Cedrick LUNVEN (@clunven)
 */
public class StargateAuthenticationService implements TokenProvider, ApiConstants {

    /** Simple Client. */
    public static RetryHttpClient httpClient = new RetryHttpClient();

    /** Default username for Cassandra. */
    public static final String DEFAULT_USERNAME      = "cassandra";
    
    /** Default password for Cassandra. */
    public static final String DEFAULT_PASSWORD      = "cassandra";
    
    /** Default URL for a Stargate node. */
    public static final String DEFAULT_AUTH_URL      = "http://localhost:8081";
    
    /** Default Timeout for Stargate token (1800s). */
    public static Duration DEFAULT_TIMEOUT_TOKEN = Duration.ofMinutes(30);

    /** Credentials. */
    private final String username;

    /** Credentials. */
    private final String password;

    /** Authentication token, time to live. */
    private final Duration tokenTtl = DEFAULT_TIMEOUT_TOKEN;
    
    /** Mark the token update. */
    private long tokenCreationTime = 0;
    
    /** Storing an authentication token to speed up queries. */
    private String token;

    /** Get Topology of the nodes. */
    protected Loadbalancer<String> endPointAuthenticationLB;

    /**  Using defaults settings. */
    public StargateAuthenticationService() {
        this(DEFAULT_USERNAME, DEFAULT_PASSWORD, DEFAULT_AUTH_URL);
    }

    /**
     * Overriding credentials.
     *
     * @param username
     *      username
     * @param password
     *      password
     */
    public StargateAuthenticationService(String username, String password) {
        this(username, password, DEFAULT_AUTH_URL);
    }

    /**
     * Credentials and auth url customize.
     *
     * @param username
     *      username
     * @param password
     *      password
     * @param url
     *      endpoint to authenticate.
     */
    public StargateAuthenticationService(String username, String password, String... url) {
        this(username, password, Arrays.asList(url));
    }

    /**
     * Full-fledged constructor.
     *
     * @param url
     *      endpoint to authenticate.
     */
    public StargateAuthenticationService(String url) {
        this(Collections.singletonList(url));
    }

    /**
     * Full-fledged constructor.
     *
     * @param url
     *      endpoint to authenticate.
     */
    public StargateAuthenticationService(List<String> url) {
        this(DEFAULT_USERNAME, DEFAULT_PASSWORD, url);
    }

    /**
     * Full-fledged constructor.
     *
     * @param username
     *      username
     * @param password
     *      password
     * @param url
     *      endpoint to authenticate.
     */
    public StargateAuthenticationService(String username, String password, List<String> url) {
        Assert.hasLength(username, "username");
        Assert.hasLength(password, "password");
        Assert.notNull(url, "Url list");
        Assert.isTrue(!url.isEmpty(), "Url list should not be empty");
        this.username = username;
        this.password = password;
        this.endPointAuthenticationLB = new Loadbalancer<>(url.toArray(new String[0]));
    }

    /**
     * Generate or renew authentication token.
     *
     * @return String
     */
    @Override
    public String getToken() {
        if ((System.currentTimeMillis() - tokenCreationTime) > 1000 * tokenTtl.getSeconds()) {
            token = renewToken();
            tokenCreationTime = System.currentTimeMillis();
        }
        return token;
    }

    /**
     * If token is null or too old (X seconds) renew the token.
     *
     * @return
     *      new value for a token
     */
    private String renewToken() {
        try {
            String body = "{"
                    + "  \"username\":" + JsonUtils.valueAsJson(username)
                    + ", \"password\":" + JsonUtils.valueAsJson(password)
                    + "}";

            HttpRequest request = HttpRequest.newBuilder()
                    .uri(new URI(endPointAuthenticationLB.get() + "/v1/auth"))
                    .method("POST", HttpRequest.BodyPublishers.ofString(body))
                    .header(HEADER_CONTENT_TYPE, CONTENT_TYPE_JSON)
                    .header(HEADER_USER_AGENT, REQUEST_WITH)
                    .header(HEADER_REQUEST_ID, UUID.randomUUID().toString())
                    .header(HEADER_REQUESTED_WITH, REQUEST_WITH)
                    .build();

            // Reuse Execute HTTP for the retry mechanism
            ApiResponseHttp response = httpClient.executeHttp(request, true);

            if (response !=null) {
                if (201 == response.getCode() || 200 == response.getCode()) {
                    return (String) JsonUtils.unmarshallBean(response.getBody(), Map.class).get("authToken");
                }
            }
            String errorMessage = (response != null) ? response.getBody() : "no response";
            throw new IllegalStateException("Cannot generate authentication token " + errorMessage);
        } catch(Exception e)  {
            throw new IllegalArgumentException("Cannot generate authentication token", e);
        }
    }

}
