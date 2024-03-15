package io.stargate.sdk.data.client.model;

import io.stargate.sdk.data.client.model.Document;
import lombok.Data;

import java.util.List;

/**
 * Subpart of the payload for json api response holding returned data.
 */
@Data
public class DataApiData {

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
    public DataApiData() {
    }
}
