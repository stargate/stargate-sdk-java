package io.stargate.test.auth;

import io.stargate.sdk.http.RetryHttpClient;
import io.stargate.sdk.auth.StargateAuthenticationService;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class AuthenticationTest {

    @Test
    public void authenticate() {
        String token = new StargateAuthenticationService().getToken();
        System.out.println("Token: " + token);
        Assertions.assertNotNull(token);
    }
}
