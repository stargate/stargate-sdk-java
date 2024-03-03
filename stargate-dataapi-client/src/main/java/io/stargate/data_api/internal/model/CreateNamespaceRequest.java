package io.stargate.data_api.internal.model;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.stargate.data_api.client.model.CreateNamespaceOptions;
import lombok.Data;

import java.util.HashMap;
import java.util.Map;

/**
 * Represents the Namespace (keyspac) definition with its name and metadata.
 */
@Data
public class CreateNamespaceRequest {

    /**
     * Replication strategies
     */
    public enum ReplicationStrategy {

        /**
         * dev
         */
        SimpleStrategy,

        /**
         * prod
         */
        NetworkTopologyStrategy
    }

    /**
     * The name of the namespace.
     */
    private String name;

    /**
     * The options of the namespace.
     */
    private CreateNamespaceOptions options;

    /**
     * Default Constructor.
     */
    public CreateNamespaceRequest() {
    }



}
