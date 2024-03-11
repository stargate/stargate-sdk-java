package io.stargate.sdk.v1.data.domain;

import lombok.Data;

import java.util.List;

/**
 * Subpart of the payload for json api response holding returned data.
 */
@Data
public class ApiData {

    /**
     * List of documents.
     */
    List<JsonDocumentResult> documents;

    /**
     * Document.
     */
    JsonDocumentResult document;

    /**
     * Next page state.
     */
    String nextPageState;

    /**
     * Default constructor.
     */
    public ApiData() {
    }
}
