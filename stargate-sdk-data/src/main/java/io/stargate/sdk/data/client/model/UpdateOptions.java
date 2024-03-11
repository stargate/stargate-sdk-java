package io.stargate.sdk.data.client.model;

import lombok.Data;

@Data
public class UpdateOptions {
    private boolean upsert;
    private Boolean bypassDocumentValidation;
}
