package io.stargate.sdk.data.domain;

import lombok.Data;

/**
 * Payload for json Api update.
 */
@Data
public class JsonResultUpdate {

   /**
    * Json document
     */
   JsonDocumentResult jsonResult;

    /**
     * Status Returned
     */
    UpdateStatus updateStatus;

    /**
     * Default constructor.
     */
    public JsonResultUpdate() {
    }
}
