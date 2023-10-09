package io.stargate.sdk.json.domain;

import lombok.Data;

@Data
public class UpdateStatus {

    /**
     * Upstarted id
     */
    private String upsertedId;

    /**
     * Number of matched documents
     */
    private Integer matchedCount;

    /**
     * Number of modified documents
     */
    private Integer modifiedCount;

    /**
     * Number of items delete
     */
    private Integer deletedCount;
}
