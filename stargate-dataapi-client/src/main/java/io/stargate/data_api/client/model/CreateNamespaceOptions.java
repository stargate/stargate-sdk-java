package io.stargate.data_api.client.model;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.stargate.data_api.internal.model.CreateNamespaceRequest;
import lombok.Data;

import java.util.HashMap;
import java.util.Map;

@Data
public class CreateNamespaceOptions {

    /**
     * The replication of the namespace.
     */
    private NamespaceReplicationOptions replication;

    /**
     * Default Constructor.
     */
    private CreateNamespaceOptions() {
        replication = new NamespaceReplicationOptions();
    }

    /**
     * The replication of the namespace.
     */
    public static class NamespaceReplicationOptions {

        /**
         * The class of the replication.
         */
        @JsonProperty("class")
        private CreateNamespaceRequest.ReplicationStrategy clazz;

        /**
         * The options of the replication.
         */
        @JsonAnyGetter
        private Map<String, Integer> strategyOptions = new HashMap<>();
        /**
         * Default constructor.
         */
        public NamespaceReplicationOptions() {
        }
    }

    public static CreateNamespaceOptions simpleStrategy(int replicationFactor) {
       CreateNamespaceOptions options = new CreateNamespaceOptions();
       options.replication.clazz = CreateNamespaceRequest.ReplicationStrategy.SimpleStrategy;
       options.replication.strategyOptions.put("replication_factor", replicationFactor);
       return options;
    }

    public static CreateNamespaceOptions networkTopologyStrategy(Map<String, Integer> datacenters) {
        CreateNamespaceOptions options = new CreateNamespaceOptions();
        options.replication.clazz = CreateNamespaceRequest.ReplicationStrategy.NetworkTopologyStrategy;
        options.replication.strategyOptions.putAll(datacenters);
        return options;
     }

}
