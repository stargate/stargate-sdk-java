package io.stargate.sdk.data.client.model.insert;

import io.stargate.sdk.data.client.DataApiClient;
import lombok.Builder;
import lombok.Data;

import static io.stargate.sdk.http.HttpClientOptions.DEFAULT_REQUEST_TIMEOUT_SECONDS;

/**
 * Options for InsertMany
 */
@Data @Builder
public class InsertManyOptions {

    /**
     * If the flag is set to true the command is failing on first error
     */
    @Builder.Default
    private boolean ordered = false;

    /**
     * If the flag is set to true the command is failing on first error
     */
    @Builder.Default
    private int concurrency = 1;

    /**
     * If the flag is set to true the command is failing on first error
     */
    @Builder.Default
    private int chunkSize = DataApiClient.MAX_DOCUMENTS_IN_INSERT;

    /**
     * If the flag is set to true the command is failing on first error
     */
    @Builder.Default
    private int timeout = DEFAULT_REQUEST_TIMEOUT_SECONDS * 1000;




}
