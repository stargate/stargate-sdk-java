package io.stargate.sdk.data.client.model.update;

import lombok.Data;

@Data
public class ReplaceOneOptions {

    /**
     * if upsert is selected
     */
    Boolean upsert = false;
}
