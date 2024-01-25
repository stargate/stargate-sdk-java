package io.stargate.sdk.data.domain;

import com.sun.jdi.InvalidStackFrameException;
import lombok.Data;

import java.util.Arrays;
import java.util.List;

/**
 * Represents the Collection definition with its name and metadata.
 */
@Data
public class CollectionDefinition {

    /**
     * Name of the collection.
     */
    private String name;

    /**
     * Options for the collection.
     */
    private Options options;

    /**
     * Default constructor.
     */
    public CollectionDefinition() {
    }

    /**
     * Gets a builder.
     *
     * @return a builder
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Indexing Options for the collection definition
     */
    @Data
    public static class Indexing {

        /**
         * If not empty will index everything but those properties.
         */
        private List<String> deny;

        /**
         * If not empty will index just those properties.
         */
        private List<String> allow;

    }

    /**
     * Options for the collection.
     */
    @Data
    public static class Options {

        /**
         * Vector options.
         */
        private Vector vector;

        /**
         * Vectorized options.
         */
        //private Vectorize vectorize;

        /**
         * Indexing options
         */
        private Indexing indexing;

        /**
         * Default constructor.
         */
        public Options() {
        }
        /**
         * Vector options.
         */
        @Data
        public static class Vector {
            /**
             * Size of the vector.
             */
            private int dimension;
            /**
             * Similarity metric.
             */
            private SimilarityMetric metric;
            /**
             * Default constructor.
             */
            public Vector() {
            }
        }

        /**
         * Vectorize options.
         */
        @Data
        public static class Vectorize {
            /**
             * Service to use.
             */
            private String service;
            /**
             * Options for the vectorize service.
             */
            private OptionsForVectorize options;
            /**
             * Default constructor.
             */
            public Vectorize() {
            }
            /**
             * Options for the vectorize service.
             */
            @Data
            public static class OptionsForVectorize {
                private String modelName;
                /**
                 * Default constructor.
                 */
                public OptionsForVectorize() {
                }
            }
        }
    }

    /**
     * Builder for {@link CollectionDefinition}.
     */
    public static class Builder {
        /**
         * Name of the collection.
         */
        String name;

        /**
         * Options for the collection.
         */
        Options options;

        /**
         * Default constructor.
         */
        public Builder() {
        }

        /**
         * Builder pattern.
         *
         * @param name
         *      name
         * @return
         *    self reference
         */
        public Builder name(String name) {
            this.name = name;
            return this;
        }

        /**
         * Builder pattern.
         *
         * @param size
         *      size
         * @return
         *    self reference
         */
        public Builder vectorDimension(int size) {
            if (options == null) {
                options = new Options();
            }
            if (options.vector == null) {
                options.vector = new Options.Vector();
            }
            options.vector.dimension = size;
            return this;
        }

        /**
         * Builder pattern.
         *
         * @param properties
         *      size
         * @return
         *    self reference
         */
        public Builder indexingDeny(String... properties) {
            if (properties != null) {
                if (options == null) {
                    options = new Options();
                }
                if (options.indexing == null) {
                    options.indexing = new Indexing();
                }
                if (options.indexing.allow !=null) {
                    throw new IllegalStateException("'indexing.deny' and 'indexing.allow' are mutually exclusive");
                }
                options.indexing.deny = Arrays.asList(properties);
            }
            return this;
        }

        /**
         * Builder pattern.
         *
         * @param properties
         *      size
         * @return
         *    self reference
         */
        public Builder indexingAllow(String... properties) {
            if (properties != null) {
                if (options.indexing == null) {
                    options.indexing = new Indexing();
                }
                if (options.indexing.deny !=null) {
                    throw new IllegalStateException("'indexing.deny' and 'indexing.allow' are mutually exclusive");
                }
                options.indexing.allow = Arrays.asList(properties);
            }
            return this;
        }


        /**
         * Builder pattern.
         *
         * @param dimension
         *      dimension
         * @param function
         *      function
         * @return
         *    self reference
         */
        public Builder vector(int dimension, SimilarityMetric function) {
            similarityMetric(function);
            vectorDimension(dimension);
            return this;
        }

        /**
         * Builder pattern.
         *
         * @param service
         *      service
         * @param modelName
         *      name
         * @return
         *    self reference
         *
        public Builder vectorize(String service, String modelName) {
            llmProvider(service);
            llmModel(modelName);
            return this;
        }*/

        /**
         * Builder pattern.
         *
         * @param function
         *      function
         * @return
         *    bself reference
         */
        public Builder similarityMetric(SimilarityMetric function) {
            if (options == null) {
                options = new Options();
            }
            if (options.vector == null) {
                options.vector = new Options.Vector();
            }
            options.vector.metric = function;
            return this;
        }

        /**
         * Builder pattern.
         *
         * @param service
         *      service
         * @return
         *    self reference
         *
        public Builder llmProvider(String service) {
            if (options == null) {
                options = new Options();
            }
            if (options.vectorize == null) {
                options.vectorize = new Options.Vectorize();
            }
            options.vectorize.service = service;
            return this;
        }

        /**
         * Builder pattern.
         *
         * @param modelName
         *      modelName
         * @return
         *    self reference
         *
        public Builder llmModel(String modelName) {
            if (options.vectorize == null) {
                options.vectorize = new Options.Vectorize();
            }
            if (options.vectorize.options == null) {
                options.vectorize.options = new Options.Vectorize.OptionsForVectorize();
            }
            options.vectorize.options.modelName = modelName;
            return this;
        }*/

        /**
         * Build the output.
         *
         * @return
         *      collection definition
         */
        public CollectionDefinition build() {
            CollectionDefinition req = new CollectionDefinition();
            req.name     = this.name;
            req.options  = this.options;
            return req;
        }

    }

}
