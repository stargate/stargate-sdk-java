package io.stargate.sdk.http;

import io.stargate.sdk.Service;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

/**
 * The target service is an HTTP Endpoint. This services can be used for
 * graphQL, rest, docs but not gRPC for instance.
 */
@Slf4j
public class ServiceHttp extends Service {

    /** Simple Client. */
    public static HttpClient healthCheckClient = HttpClient.newHttpClient();

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
        try {
            log.debug("IS_ALIVE(" + endpoint + ")");
            int code = healthCheckClient.send(HttpRequest.newBuilder(
                    new URI(healthCheckEndpoint)).GET().build(),
                    HttpResponse.BodyHandlers.discarding()).statusCode();
            log.debug("CODE(" + healthCheckEndpoint + ")" + code);
            return HttpURLConnection.HTTP_OK == code;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
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
