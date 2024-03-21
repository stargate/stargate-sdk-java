package io.stargate.sdk.data.client.model.misc;

import lombok.Data;

@Data
public final class BulkWriteOptions {

    private boolean ordered = true;

    private Integer concurrency = 5;
}