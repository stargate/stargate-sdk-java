package io.stargate.sdk.data.client.model.collections;

import io.stargate.sdk.data.client.model.collections.CreateCollectionOptions;
import io.stargate.sdk.utils.JsonUtils;
import lombok.Data;

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
    private CreateCollectionOptions options;

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return JsonUtils.marshallForDataApi(this);
    }
}
