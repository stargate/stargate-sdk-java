package io.stargate.sdk.json.domain;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Data;

/**
 * Payload for json Api update.
 */
@Data
public class JsonResultUpdate {

   /**
    * Json document
     */
   JsonResult jsonResult;

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
