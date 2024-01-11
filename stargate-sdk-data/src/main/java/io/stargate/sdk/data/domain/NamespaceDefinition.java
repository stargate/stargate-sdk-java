package io.stargate.sdk.data.domain;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

import java.util.HashMap;
import java.util.Map;

/**
 * Pogo to create a namspace.
 */
@Data
public class NamespaceDefinition {

    /**
     * Replication strategies
     */
    public static enum ReplicationStrategy {

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
    private Options options;

    /**
     * Default Constructor.
     */
    public NamespaceDefinition() {
    }

    /**
     * Create a builder to create a namespace.
     *
     * @return
     *      builder
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * The options of the namespace.
     */
    @Data
    public static class Options {
        /**
         * The replication of the namespace.
         */
        private Replication replication;
        /**
         * Default Constructor.
         */
        public Options() {
        }
    }

    /**
     * The replication of the namespace.
     */
    public static class Replication {
        /**
         * The class of the replication.
         */
        @JsonProperty("class")
        private ReplicationStrategy clazz;
        /**
         * The options of the replication.
         */
        @JsonAnyGetter
        private Map<String, Integer> strategyOptions = new HashMap<>();
        /**
         * Default constructor.
         */
        public Replication() {
        }
    }

    /**
     * Builder to create a namespace.
     */
    public static class Builder {

        /**
         * The namespace to create.
         */
        final NamespaceDefinition config;

        /**
         * Default constructor.
         */
        public Builder() {
            this.config = new NamespaceDefinition();
        }

        /**
         * Builder Pattern.
         *
         * @param name
         *      update name
         * @return
         *      self reference
         */
        public Builder name(String name) {
            this.config.name = name;
            return this;
        }

        /**
         * Builder Pattern.
         *
         * @param replicationFactor
         *      update replicationFactor
         * @return
         *      self reference
         */
        public Builder simpleStrategy(int replicationFactor) {
           replicationStrategy(ReplicationStrategy.SimpleStrategy);
           withOption("replication_factor", replicationFactor);
           return this;
        }

        /**
         * Builder Pattern.
         *
         * @param datacenters
         *      update datacenters
         * @return
         *      self reference
         */
        public Builder networkTopologyStrategy(Map<String, Integer> datacenters) {
            replicationStrategy(ReplicationStrategy.NetworkTopologyStrategy);
            datacenters.forEach((dc, rf) -> withOption(dc, rf));
            return this;
        }

        /**
         * Builder Pattern.
         *
         * @param clazz
         *      clazz name
         * @return
         *      self reference
         */
        public Builder replicationStrategy(ReplicationStrategy clazz) {
            if (this.config.options == null) {
                this.config.options = new Options();
                this.config.options.replication = new Replication();
            }
            this.config.options.replication.clazz = clazz;
            return this;
        }


        /**
         * Builder Pattern.
         *
         * @param key
         *      updated key
         * @param value
         *      value key
         * @return
         *      self reference
         */
        public Builder withOption(String key, Integer value) {
            if (this.config.options == null) {
                this.config.options = new Options();
                this.config.options.replication = new Replication();
            }
            this.config.options.replication.strategyOptions.put(key, value);
            return this;
        }

        /**
         * Build the namespace.
         *
         * @return
         *      namespace
         */
        public NamespaceDefinition build() {
            return this.config;
        }
    }

}
