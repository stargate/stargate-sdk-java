package io.stargate.sdk.grpc;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.stargate.sdk.Service;
import io.stargate.sdk.api.ApiConstants;
import lombok.Getter;

import java.net.HttpURLConnection;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.concurrent.TimeUnit;

/**
 * Implementation.
 */
@Getter
public class ServiceGrpc extends Service {

    /** Simple Client. */
    public static HttpClient healthCheckClient = HttpClient.newHttpClient();

    /** Secured transport. */
    protected boolean securedTransport;

    /** Retries. */
    protected int maxRetries = 3;

    /** Keep Alive. */
    protected long keepAliveTimeout = 100000;

    /** Keep Alive. */
    protected TimeUnit keepAliveTimeoutUnit = TimeUnit.MILLISECONDS;

    /** Channel. */
    private  ManagedChannel channel;

    /**
     * Constructor.
     * @param id                  identifier
     * @param endpoint            endpoint
     * @param healthCheckEndpoint health check
     */
    public ServiceGrpc(String id, String endpoint, String healthCheckEndpoint) {
        this(id, endpoint, healthCheckEndpoint, false);
    }

    /**
     * Constructor.
     * @param id                  identifier
     * @param endpoint            endpoint
     * @param healthCheckEndpoint health check
     * @param securedTransport    s
     */
    public ServiceGrpc(String id, String endpoint, String healthCheckEndpoint, boolean securedTransport) {
        super(id, endpoint, healthCheckEndpoint);
        this.securedTransport = securedTransport;
        initialize();
    }

   /**
    * Initialize grpc Channel based on configuration.
    **/
    private void initialize() {
        ManagedChannelBuilder<?> channelBuilder = ManagedChannelBuilder
                .forTarget(endpoint)
                //.forAddress("localhost", 8090)
                .enableRetry()
                // Apply same try policy
                .maxRetryAttempts(maxRetries)
                // Apply same keep alive timeout
                .keepAliveTimeout(keepAliveTimeout, keepAliveTimeoutUnit)
                // Apply headers
                .userAgent(ApiConstants.REQUEST_WITH);
        // Astra = 443 so secure transport
        if (securedTransport) {
            channelBuilder.useTransportSecurity();
        } else {
            channelBuilder.usePlaintext();
        }
        this.channel = channelBuilder.build();
    }

    /**
     * Check that a service is alive.
     *
     * @return
     *      validate that the current service is alive
     */
    @Override
    public boolean isAlive() {
        try {
            return HttpURLConnection.HTTP_OK == healthCheckClient.send(
                    HttpRequest.newBuilder(new URI(healthCheckEndpoint)).GET().build(),
                    HttpResponse.BodyHandlers.discarding()).statusCode();
        } catch (Exception e) {
            return false;
        }
    }

}
