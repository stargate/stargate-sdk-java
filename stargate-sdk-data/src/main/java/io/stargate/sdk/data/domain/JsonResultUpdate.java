package io.stargate.sdk.data.domain;

import lombok.Data;

/**
 * Represents the payload returned for a specialized update function.
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
