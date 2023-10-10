package io.stargate.sdk.json.vector;

import java.util.List;

/**
 * Vector Embeddings.
 */
public class VectorEmbeddings {

    public static float[] of(List<Float> embeddings) {
        float[] vector = new float[embeddings.size()];
        for (int i = 0; i < embeddings.size(); i++) {
            vector[i] = embeddings.get(i);
        }
        return vector;
    }

    public static float[] of(Float[] embeddings) {
        float[] vector = new float[embeddings.length];
        for (int i = 0; i < embeddings.length; i++) {
            vector[i] = embeddings[i];
        }
        return vector;
    }
}
