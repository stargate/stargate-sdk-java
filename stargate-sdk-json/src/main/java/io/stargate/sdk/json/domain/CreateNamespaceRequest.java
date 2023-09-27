package io.stargate.sdk.json.domain;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Builder;
import lombok.Data;
import lombok.Getter;

import java.util.HashMap;
import java.util.Map;

@Data
public class CreateNamespaceRequest {

    public static enum ReplicationStrategy { SimpleStrategy, NetworkTopologyStrategy }

    private String name;

    private Options options;

    public static Builder builder() {
        return new Builder();
    }

    @Data
    public static class Options {
        private Replication replication;
    }

    public static class Replication {

        @JsonProperty("class")
        private ReplicationStrategy clazz;

        @JsonAnyGetter
        private Map<String, Integer> strategyOptions = new HashMap<>();
    }

    public static class Builder {

        private final CreateNamespaceRequest config;

        public Builder() {
            this.config = new CreateNamespaceRequest();
        }

        public Builder name(String name) {
            this.config.name = name;
            return this;
        }

        public Builder replicationStrategy(ReplicationStrategy clazz) {
            if (this.config.options == null) {
                this.config.options = new Options();
                this.config.options.replication = new Replication();
            }
            this.config.options.replication.clazz = clazz;
            return this;
        }

        public Builder withOption(String key, Integer value) {
            if (this.config.options == null) {
                this.config.options = new Options();
                this.config.options.replication = new Replication();
            }
            this.config.options.replication.strategyOptions.put(key, value);
            return this;
        }

        public CreateNamespaceRequest build() {
            return this.config;
        }
    }

}
