package io.stargate.sdk.http.auth;

import io.stargate.sdk.api.ApiConstants;
import io.stargate.sdk.api.TokenProvider;
import io.stargate.sdk.http.RetryHttpClient;
import io.stargate.sdk.http.domain.ApiResponseHttp;
import io.stargate.sdk.loadbalancer.Loadbalancer;
import io.stargate.sdk.utils.Assert;
import io.stargate.sdk.utils.JsonUtils;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.core5.http.io.entity.StringEntity;

import java.time.Duration;
import java.util.*;

/**
 * Using the authentication endpoint you shoud be able tp...
 *
 * @author Cedrick LUNVEN (@clunven)
 */
public class TokenProviderHttpAuth implements TokenProvider, ApiConstants {

    /** default service id. */
    private static final String DEFAULT_SERVICE_ID = "sgv2-auth";

    /** Default username for Cassandra. */
    public static final String DEFAULT_USERNAME      = "cassandra";
    
    /** Default password for Cassandra. */
    public static final String DEFAULT_PASSWORD      = "cassandra";
    
    /** Default URL for a Stargate node. */
    public static final String DEFAULT_AUTH_URL      = "http://localhost:8081";
    
    /** Defualt Timeout for Stargate token (1800s). */
    public static Duration DEFAULT_TIMEOUT_TOKEN = Duration.ofMinutes(30);

    /** Credentials. */
    private String username = DEFAULT_USERNAME;

    /** Credentials. */
    private String password = DEFAULT_PASSWORD;

    /** Authentication token, time to live. */
    private Duration tokenttl = DEFAULT_TIMEOUT_TOKEN;
    
    /** Mark the token update. */
    private long tokenCreatedtime = 0;
    
    /** Storing an authentication token to speed up queries. */
    private String token;

    /** Get Topology of the nodes. */
    protected Loadbalancer<String> endPointAuthenticationLB;

    /**
     * Using defautls
     */
    public TokenProviderHttpAuth() {
        this(DEFAULT_USERNAME, DEFAULT_PASSWORD, DEFAULT_AUTH_URL);
    }

    /**
     * Overrriding credentials.
     *
     * @param username
     *      username
     * @param password
     *      password
     */
    public TokenProviderHttpAuth(String username, String password) {
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
    public TokenProviderHttpAuth(String username, String password, String... url) {
        this(username, password, Arrays.asList(url));
    }

    /**
     * Full fledge constructor.
     *
     * @param username
     *      username
     * @param password
     *      password
     * @param url
     *      endpoint to authenticate.
     */
    public TokenProviderHttpAuth(String username, String password, String url) {
        this(Collections.singletonList(url));
    }

    /**
     * Full fledge constructor.
     *
     * @param url
     *      endpoint to authenticate.
     */
    public TokenProviderHttpAuth(List<String> url) {
        this(DEFAULT_USERNAME, DEFAULT_PASSWORD, url);
    }

    /**
     * Full fledge constructor.
     *
     * @param username
     *      username
     * @param password
     *      password
     * @param url
     *      endpoint to authenticate.
     */
    public TokenProviderHttpAuth(String username, String password, List<String> url) {
        Assert.hasLength(username, "username list shoudl not be null");
        Assert.hasLength(password, "password list shoudl not be null");
        Assert.notNull(url, "Url list shoudl not be null");
        Assert.isTrue(url.size()>0, "Url list should not be empty");
        this.username = username;
        this.password = password;
        this.endPointAuthenticationLB = new Loadbalancer<String>(url.toArray(new String[0]));
    }

    /**
     * Generate or renew authentication token.
     *
     * @return String
     */
    @Override
    public String getToken() {
        if ((System.currentTimeMillis() - tokenCreatedtime) > 1000 * tokenttl.getSeconds()) {
            token = renewToken();
            tokenCreatedtime = System.currentTimeMillis();
        }
        return token;
    }

    /**
     * If token is null or too old (X seconds) renew the token.
     *
     * @return
     */
    private String renewToken() {
        try {
            HttpPost httpPost = new HttpPost(endPointAuthenticationLB.get() + "/v1/auth");
            httpPost.addHeader(HEADER_CONTENT_TYPE, CONTENT_TYPE_JSON);
            httpPost.addHeader(HEADER_USER_AGENT, REQUEST_WITH);
            httpPost.addHeader(HEADER_REQUEST_ID, UUID.randomUUID().toString());
            httpPost.addHeader(HEADER_REQUESTED_WITH, REQUEST_WITH);
            httpPost.setEntity(new StringEntity(new StringBuilder("{")
                    .append("\"username\":").append(JsonUtils.valueAsJson(username))
                    .append(", \"password\":").append(JsonUtils.valueAsJson(password))
                    .append("}").toString()
            ));
            // Reuse Execute HTTP for the retry mechanism
            ApiResponseHttp response = RetryHttpClient.getInstance().executeHttp(null, httpPost, true);

            if (response != null && 201 == response.getCode() || 200 == response.getCode()) {
                return (String) JsonUtils.unmarshallBean(response.getBody(), Map.class).get("authToken");
            } else {
                throw new IllegalStateException("Cannot generate authentication token " + response.getBody());
            }

        } catch(Exception e)  {
            throw new IllegalArgumentException("Cannot generate authentication token", e);
        }
    }

}
