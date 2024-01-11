package io.stargate.sdk.data.domain;

/**
 * Similarity metric.
 */
public enum SimilarityMetric {
    /**Distance with cosine. */
    cosine,
    /** Distance with Euclidean (L2). */
    euclidean,
    /**Distance with dot+product (L1). */
    dot_product
}
