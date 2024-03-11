package io.stargate.sdk.v1.data.domain;

import lombok.Data;

/**
 * Payload of Api when updating documents.
 */
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

    /**
     * Default constructor.
     */
    public UpdateStatus() {
    }

}
