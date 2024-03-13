package io.stargate.sdk.data.client.model.insert;

import lombok.Data;

@Data
public class InsertManyOptions {
    private boolean ordered = true;
    private Boolean bypassDocumentValidation;
}
