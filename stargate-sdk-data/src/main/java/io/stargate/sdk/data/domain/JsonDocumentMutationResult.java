package io.stargate.sdk.data.domain;

import java.util.Map;

/**
 * Result per document working with a schemaless document (JsonDocument or Json String).
 */
public class JsonDocumentMutationResult extends DocumentMutationResult<Map<String, Object>> {

    /**
     * Default Constructor
     */
    public JsonDocumentMutationResult() {
        super();
    }

    /**
     * Constructor with document.
     *
     * @param doc
     *      current document
     */
    public JsonDocumentMutationResult(JsonDocument doc) {
        super(doc);;
    }

    /**
     * Constructor with document.
     *
     * @param doc
     *      current document
     * @param status
     *      current status
     */
    public JsonDocumentMutationResult(JsonDocument doc, DocumentMutationStatus status) {
        super(doc, status);;
    }

    /**
     * Return document.
     *
     * @return
     *      ccurent document
     */
    @Override
    public JsonDocument getDocument() {
        JsonDocument doc = new JsonDocument();
        doc.id(this.document.getId());
        doc.data(this.document.getData());
        doc.vector(this.document.getVector());
        return doc;
    }
}
