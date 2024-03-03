package io.stargate.data_api.client;


import io.stargate.data_api.internal.DataApiClientImpl;
import io.stargate.sdk.ServiceDeployment;
import io.stargate.sdk.api.SimpleTokenProvider;
import io.stargate.sdk.api.TokenProvider;
import io.stargate.sdk.http.ServiceHttp;
import io.stargate.sdk.http.auth.TokenProviderHttpAuth;
import lombok.NonNull;

/**
 * Initialization of the client in a Static way.
 */
public class DataApiClients {

    /**
     * Utility class, should not be instanced.
     */
    private DataApiClients() {}

    /**
     * Create from an Endpoint only
     */
    public static DataApiClient create() {
        return new DataApiClientImpl(
                DataApiClientImpl.DEFAULT_ENDPOINT,
                DataApiClientImpl.DEFAULT_VERSION,
                new TokenProviderHttpAuth());
    }

    /**
     * Create from an Endpoint only
     *
     * @param endpoint
     *      service endpoint
     * @param token
     *      token
     */
    public static DataApiClient create(@NonNull String endpoint, @NonNull String token) {
        return new DataApiClientImpl(endpoint,  DataApiClientImpl.DEFAULT_VERSION, new SimpleTokenProvider(token));
    }

    /**
     * Initialized document API with a URL and a token.
     *
     * @param serviceDeployment
     *      http client topology aware
     */
    public static DataApiClient create(ServiceDeployment<ServiceHttp> serviceDeployment) {
        return new DataApiClientImpl(serviceDeployment);
    }

}
