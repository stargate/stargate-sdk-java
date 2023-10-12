package io.stargate.sdk.json.domain;

import lombok.Data;

import java.util.List;

/**
 * Payload for json api response.
 */
@Data
public class JsonApiData {

    /**
     * List of documents.
     */
    List<JsonResult> documents;

    /**
     * Document.
     */
    JsonResult document;

    /**
     * Next page state.
     */
    String nextPageState;

    /**
     * Default constructor.
     */
    public JsonApiData() {
    }
}
