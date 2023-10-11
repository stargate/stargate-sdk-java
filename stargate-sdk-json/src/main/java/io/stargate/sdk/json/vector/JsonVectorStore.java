package io.stargate.sdk.json.vector;

import io.stargate.sdk.core.domain.ObjectMap;
import io.stargate.sdk.json.JsonCollectionClient;

/**
 * Generic Store.
 */
public class JsonVectorStore extends VectorStore<ObjectMap> {

    /**
     * Default constructor.
     *
     * @param col   collection client parent
     */
    public JsonVectorStore(JsonCollectionClient col) {
        super(col, ObjectMap.class);
    }


}
