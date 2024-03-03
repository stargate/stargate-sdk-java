package io.stargate.data_api.internal.model;

import io.stargate.data_api.client.model.Document;
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
    List<Document> documents;

    /**
     * Document.
     */
    Document document;

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
