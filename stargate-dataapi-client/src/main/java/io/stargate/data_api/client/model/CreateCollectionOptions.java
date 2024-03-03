package io.stargate.data_api.client.model;

import io.stargate.data_api.internal.model.CreateCollectionRequest;
import lombok.Data;
import lombok.NonNull;

import java.util.Arrays;
import java.util.List;

@Data
public class CreateCollectionOptions {

    /**
     * Vector options.
     */
    private VectorOptions vector;

    /**
     * Indexing options
     */
    private IndexingOptions indexing;

    @Data
    public static class IndexingOptions {

        /**
         * If not empty will index everything but those properties.
         */
        private List<String> deny;

        /**
         * If not empty will index just those properties.
         */
        private List<String> allow;

        /**
         * Default constructor.
         */
        public IndexingOptions() {}
    }

    @Data
    static public class VectorOptions {

        /**
         * Size of the vector.
         */
        private int dimension;

        /**
         * Similarity metric.
         */
        private SimilarityMetric metric;

        /**
         * Service for vectorization
         */
        private Service service;
    }

    @Data
    public static class Service {
        private String provider;
        private String modelName;
        private Authentication authentication;
        private Parameters parameters;
    }

    @Data
    public static class Authentication {
        private List<String> type;
        private String secretName;
    }

    @Data
    public static class Parameters {
        private String projectId;
    }

    /**
     * Gets a builder.
     *
     * @return a builder
     */
    public static CreateCollectionOptionsBuilder builder() {
        return new CreateCollectionOptionsBuilder();
    }

    /**
     * Builder for {@link CreateCollectionRequest}.
     */
    public static class CreateCollectionOptionsBuilder {

        /**
         * Options for the collection.
         */
        VectorOptions vector;

        IndexingOptions indexing;

        private VectorOptions getVector() {
            if (vector == null) {
                vector = new VectorOptions();
            }
            return vector;
        }

        private IndexingOptions getIndexing() {
            if (indexing == null) {
                indexing = new IndexingOptions();
            }
            return indexing;
        }

        /**
         * Default constructor.
         */
        public CreateCollectionOptionsBuilder() {
        }

        /**
         * Builder pattern.
         *
         * @param size size
         * @return self reference
         */
        public CreateCollectionOptionsBuilder withVectorDimension(int size) {
            getVector().setDimension(size);
            return this;
        }

        /**
         * Builder pattern.
         *
         * @param function function
         * @return bself reference
         */
        public CreateCollectionOptionsBuilder withVectorSimilarityMetric(@NonNull SimilarityMetric function) {
            getVector().setMetric(function);
            return this;
        }

        /**
         * Builder pattern.
         *
         * @param properties size
         * @return self reference
         */
        public CreateCollectionOptionsBuilder withIndexingDeny(@NonNull String... properties) {
            if (getIndexing().getAllow() != null) {
                throw new IllegalStateException("'indexing.deny' and 'indexing.allow' are mutually exclusive");
            }
            getIndexing().setDeny(Arrays.asList(properties));
            return this;
        }

        /**
         * Builder pattern.
         *
         * @param properties size
         * @return self reference
         */
        public CreateCollectionOptionsBuilder withIndexingAllow(String... properties) {
            if (getIndexing().getDeny() != null) {
                throw new IllegalStateException("'indexing.deny' and 'indexing.allow' are mutually exclusive");
            }
            getIndexing().setAllow(Arrays.asList(properties));
            return this;
        }

        /**
         * Builder pattern.
         *
         * @param dimension dimension
         * @param function  function
         * @return self reference
         */
        public CreateCollectionOptionsBuilder vector(int dimension, @NonNull SimilarityMetric function) {
            withVectorSimilarityMetric(function);
            withVectorDimension(dimension);
            return this;
        }

        /**
         * Build the output.
         *
         * @return collection definition
         */
        public CreateCollectionOptions build() {
            CreateCollectionOptions req = new CreateCollectionOptions();
            req.vector = this.vector;
            req.indexing = this.indexing;
            return req;
        }
    }

}
