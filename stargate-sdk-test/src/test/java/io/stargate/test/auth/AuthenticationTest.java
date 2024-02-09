package io.stargate.test.auth;

import io.stargate.sdk.api.TokenProvider;
import io.stargate.sdk.http.RetryHttpClient;
import io.stargate.sdk.http.auth.TokenProviderHttpAuth;
import io.stargate.sdk.http.domain.UserAgentChunk;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class AuthenticationTest {

    @Test
    public void authenticate() {
        String token = new TokenProviderHttpAuth().getToken();
        System.out.println("Token: " + token);
        Assertions.assertNotNull(token);
    }

    @Test
    public void userAgentTest() {
        RetryHttpClient http = RetryHttpClient.getInstance();
        http.pushUserAgent(new UserAgentChunk("astra-db-client", "1.2.5"));
        http.pushUserAgent(new UserAgentChunk("langchain4j", "0.27.1"));
        System.out.println(http.getUserAgentHeader());

    }
}
