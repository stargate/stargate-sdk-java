package io.stargate.sdk.http;

import io.stargate.sdk.Service;
import org.apache.hc.client5.http.classic.methods.HttpGet;

import java.net.HttpURLConnection;

/**
 * The target service is an HTTP Endpoint. This services can be used for
 * graphQL, rest, docs but not gRPC for instance.
 */
public class ServiceHttp extends Service {

    /**
     * Constructor.
     * @param id                  identifier
     * @param endpoint            endpoint
     * @param healthCheckEndpoint health check
     */
    public ServiceHttp(String id, String endpoint, String healthCheckEndpoint) {
        super(id, endpoint, healthCheckEndpoint);
    }

    /**
     * Check that a service is alive.
     *
     * @return
     *      validate that the current service is alive
     */
    @Override
    public boolean isAlive() {
        System.out.println("IS_ALIVE(" + endpoint + ")");
        int code = RetryHttpClient
                .getInstance()
                .executeHttp(this, new HttpGet(healthCheckEndpoint), false)
                .getCode();
        System.out.println("CODE(" + healthCheckEndpoint + ")" + code);
        return HttpURLConnection.HTTP_OK == code;
    }

    @Override
    public String toString() {
        return "ServiceHttp{" +
                "id='" + id + '\'' +
                ", endpoint='" + endpoint + '\'' +
                ", healthCheckEndpoint='" + healthCheckEndpoint + '\'' +
                '}';
    }
}
