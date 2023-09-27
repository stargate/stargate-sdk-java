package io.stargate.test.auth;

import io.stargate.sdk.api.TokenProvider;
import io.stargate.sdk.http.auth.TokenProviderHttpAuth;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class AuthenticationTest {

    @Test
    public void authenticate() {
        String token = new TokenProviderHttpAuth().getToken();
        System.out.println("Token: " + token);
        Assertions.assertNotNull(token);
    }
}
