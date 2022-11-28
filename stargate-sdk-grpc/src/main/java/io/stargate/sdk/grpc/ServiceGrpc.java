package io.stargate.sdk.grpc;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.stargate.sdk.Service;
import io.stargate.sdk.api.ApiConstants;
import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.impl.classic.HttpClients;

import java.net.HttpURLConnection;
import java.util.concurrent.TimeUnit;

/**
 * Implementation.
 */
public class ServiceGrpc extends Service {

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
            return HttpURLConnection.HTTP_OK == HttpClients
                    .createDefault()
                    .execute(new HttpGet(healthCheckEndpoint))
                    .getCode();
        } catch(Exception re) {
            return false;
        }
    }

    /**
     * Set value for maxRetries
     *
     * @param maxRetries new value for maxRetries
     */
    public void setMaxRetries(int maxRetries) {
        this.maxRetries = maxRetries;
    }

    /**
     * Set value for keepAliveTimeout
     *
     * @param keepAliveTimeout new value for keepAliveTimeout
     */
    public void setKeepAliveTimeout(long keepAliveTimeout) {
        this.keepAliveTimeout = keepAliveTimeout;
    }

    /**
     * Set value for keepAliveTimeoutUnit
     *
     * @param keepAliveTimeoutUnit new value for keepAliveTimeoutUnit
     */
    public void setKeepAliveTimeoutUnit(TimeUnit keepAliveTimeoutUnit) {
        this.keepAliveTimeoutUnit = keepAliveTimeoutUnit;
    }

    /**
     * Gets channel
     *
     * @return value of channel
     */
    public ManagedChannel getChannel() {
        return channel;
    }
}
