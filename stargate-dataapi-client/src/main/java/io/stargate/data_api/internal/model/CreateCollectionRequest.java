package io.stargate.data_api.internal.model;

import io.stargate.data_api.client.model.CreateCollectionOptions;
import io.stargate.sdk.utils.JsonUtils;
import lombok.Data;

/**
 * Represents the Collection definition with its name and metadata.
 */
@Data
public class CreateCollectionRequest {

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
