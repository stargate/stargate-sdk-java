package io.stargate.sdk.data.domain;

import lombok.Data;

import java.util.List;

/**
 * Payload for json api response.
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
