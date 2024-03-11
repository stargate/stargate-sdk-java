package io.stargate.sdk.data.client.model;

import lombok.Data;

@Data
public final class BulkWriteOptions {
    private boolean ordered = true;
    private Boolean bypassDocumentValidation;
}