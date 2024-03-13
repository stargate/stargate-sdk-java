package io.stargate.sdk.data.client.model.update;

import lombok.Data;

@Data
public class ReplaceOptions {
    private boolean upsert;
    private Boolean bypassDocumentValidation;
}
