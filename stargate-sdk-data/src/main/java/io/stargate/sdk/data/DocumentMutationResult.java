package io.stargate.sdk.data;

import io.stargate.sdk.data.domain.JsonDocument;
import io.stargate.sdk.data.domain.odm.Document;
import io.stargate.sdk.utils.JsonUtils;
import lombok.Getter;
import lombok.Setter;

import java.util.Map;

/**
 * Wrapper to get result from API with schemaless document.
 *
 * @param <T>
 *     represents the pojo for returned document
 */
@Getter @Setter
public class DocumentMutationResult<T> {

    /**
     * Internal Schemaless document.
     */
    Document<T> document;

    /**
     * Status returned by the API.
     */
    DocumentMutationStatus status;

    /**
     * Default Constructor
     */
    public DocumentMutationResult() {
        this.status = DocumentMutationStatus.NOT_PROCESSED;
    }

    /**
     * Constructor with document.
     *
     * @param doc
     *      current document
     */
    public DocumentMutationResult(Document<T> doc) {
        this(doc, DocumentMutationStatus.NOT_PROCESSED);
    }

    /**
     * Constructor with document and status.
     *
     * @param doc
     *      current document
     * @param status
     *     current status
     */
    public DocumentMutationResult(Document<T> doc, DocumentMutationStatus status) {
        this.document = doc;
        this.status = status;
    }

    /**
     * Helps with the conversion of the document to a JsonDocumentMutationResult.
     *
     * @return
     *      current JsonDocumentMutationResult
     */
    @SuppressWarnings("unchecked")
    public JsonDocumentMutationResult asJsonDocumentMutationResult() {
        JsonDocument doc = new JsonDocument();
        doc.id(this.document.getId());
        doc.data(JsonUtils.convertValueForDataApi(this.document.getData(), Map.class));
        return new JsonDocumentMutationResult(doc, this.status);
    }
}
