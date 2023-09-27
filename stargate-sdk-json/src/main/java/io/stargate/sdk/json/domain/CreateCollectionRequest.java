package io.stargate.sdk.json.domain;

import lombok.Data;

/**
 * Request to create a collection.
 */
@Data
public class CreateCollectionRequest {

    private String name;
    private Options options;

    public static Builder builder() {
        return new Builder();
    }

    public static enum SimilarityMetric { cosine, euclidean, dot_product}

    public static enum LLMProvider { openai, vertex_ai, hugging_face }

    @Data
    public static class Options {
        private Vector vector;
        private Vectorize vectorize;

        @Data
        public static class Vector {
            private int size;
            private SimilarityMetric function;
        }

        @Data
        public static class Vectorize {
            private LLMProvider service;
            private OptionsForVectorize options;
            @Data
            public static class OptionsForVectorize {
                private String modelName;
            }
        }
    }

    public static class Builder {
        private String name;
        private Options options;

        public Builder name(String name) {
            this.name = name;
            return this;
        }

        public Builder vectorDimension(int size) {
            if (options == null) {
                options = new Options();
            }
            if (options.vector == null) {
                options.vector = new Options.Vector();
            }
            options.vector.size = size;
            return this;
        }

        public Builder similarityMetric(SimilarityMetric function) {
            if (options == null) {
                options = new Options();
            }
            if (options.vector == null) {
                options.vector = new Options.Vector();
            }
            options.vector.function = function;
            return this;
        }

        public Builder llmProvider(LLMProvider service) {
            if (options == null) {
                options = new Options();
            }
            if (options.vectorize == null) {
                options.vectorize = new Options.Vectorize();
            }
            options.vectorize.service = service;
            return this;
        }

        public Builder llmModel(String modelName) {
            if (options.vectorize == null) {
                options.vectorize = new Options.Vectorize();
            }
            if (options.vectorize.options == null) {
                options.vectorize.options = new Options.Vectorize.OptionsForVectorize();
            }
            options.vectorize.options.modelName = modelName;
            return this;
        }

        public CreateCollectionRequest build() {
            CreateCollectionRequest req = new CreateCollectionRequest();
            req.name = this.name;
            req.options = this.options;
            return req;
        }

    }

}
