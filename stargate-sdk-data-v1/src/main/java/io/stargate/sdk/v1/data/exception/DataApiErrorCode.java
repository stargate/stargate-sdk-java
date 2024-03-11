package io.stargate.sdk.v1.data.exception;

/**
 * All errro codes.
 */
public enum DataApiErrorCode {

    /** error. */
    COMMAND_NOT_IMPLEMENTED("The provided command is not implemented."),
    /** error. */
    COMMAND_ACCEPTS_NO_OPTIONS("Command accepts no options"),
    /** error. */
    CONCURRENCY_FAILURE("Unable to complete transaction due to concurrent transactions"),
    /** error. */
    COLLECTION_NOT_EXIST("Collection does not exist, collection name: "),
    /** error. */
    DATASET_TOO_BIG("Response data set too big to be sorted, add more filters"),
    /** error. */
    DOCUMENT_ALREADY_EXISTS("Document already exists with the given _id"),
    /** error. */
    DOCUMENT_UNPARSEABLE("Unable to parse the document"),
    /** error. */
    DOCUMENT_REPLACE_DIFFERENT_DOCID(
            "The replace document and document resolved using filter have different _id"),
    /** error. */
    FILTER_UNRESOLVABLE("Unable to resolve the filter"),
    /** error. */
    UNINDEXED_FILTER_PATH("Unindexed filter path."),
    /** error. */
    FILTER_MULTIPLE_ID_FILTER(
            "Should only have one _id filter, document id cannot be restricted by more than one relation if it includes an Equal"),
    /** error. */
    FILTER_FIELDS_LIMIT_VIOLATION("Filter fields size limitation violated"),
    /** error. */
    NAMESPACE_DOES_NOT_EXIST("The provided namespace does not exist."),
    /** error. */
    SHRED_BAD_DOCUMENT_TYPE("Bad document type to shred"),
    /** error. */
    SHRED_BAD_DOCID_TYPE("Bad type for '_id' property"),
    /** error. */
    SHRED_BAD_DOCUMENT_VECTOR_TYPE("Bad $vector document type to shred "),
    /** error. */
    SHRED_BAD_DOCUMENT_VECTORIZE_TYPE("Bad $vectorize document type to shred "),
    /** error. */
    SHRED_BAD_DOCID_EMPTY_STRING("Bad value for '_id' property: empty String not allowed"),
    /** error. */
    SHRED_INTERNAL_NO_PATH("Internal: path being built does not point to a property or element"),
    /** error. */
    SHRED_NO_MD5("MD5 Hash algorithm not available"),
    /** error. */
    SHRED_UNRECOGNIZED_NODE_TYPE("Unrecognized JSON node type in input document"),
    /** error. */
    SHRED_DOC_LIMIT_VIOLATION("Document size limitation violated"),
    /** error. */
    SHRED_DOC_KEY_NAME_VIOLATION("Document key name constraints violated"),
    /** error. */
    SHRED_BAD_EJSON_VALUE("Bad EJSON value"),
    /** error. */
    SHRED_BAD_VECTOR_SIZE("$vector field can't be empty"),
    /** error. */
    SHRED_BAD_VECTOR_VALUE("$vector search needs to be array of numbers"),
    /** error. */
    SHRED_BAD_VECTORIZE_VALUE("$vectorize search needs to be text value"),
    /** error. */
    INVALID_FILTER_EXPRESSION("Invalid filter expression"),
    /** error. */
    INVALID_COLLECTION_NAME("Invalid collection name "),
    /** error. */
    INVALID_JSONAPI_COLLECTION_SCHEMA("Not a valid json api collection schema: "),
    /** error. */
    TOO_MANY_COLLECTIONS("Too many collections"),
    /** error. */
    UNSUPPORTED_FILTER_DATA_TYPE("Unsupported filter data type"),
    /** error. */
    UNSUPPORTED_FILTER_OPERATION("Unsupported filter operator"),
    /** error. */
    INVALID_SORT_CLAUSE_PATH("Invalid sort clause path"),
    /** error. */
    INVALID_SORT_CLAUSE_VALUE(
            "Sort ordering value can only be `1` for ascending or `-1` for descending."),

    /** error. */
    INVALID_USAGE_OF_VECTORIZE("`$vectorize` and `$vector` can't be used together."),
    /** error. */
    UNSUPPORTED_OPERATION("Unsupported operation class"),
    /** error. */
    UNSUPPORTED_PROJECTION_PARAM("Unsupported projection parameter"),
    /** error. */
    UNSUPPORTED_UPDATE_DATA_TYPE("Unsupported update data type"),
    /** error. */
    UNSUPPORTED_UPDATE_OPERATION("Unsupported update operation"),
    /** error. */
    UNSUPPORTED_COMMAND_EMBEDDING_SERVICE(
            "Unsupported command `createEmbeddingService` since application is configured for property based embedding"),
    /** error. */
    UNAVAILABLE_EMBEDDING_SERVICE("Unable to vectorize data, embedding service not available"),
    /** error. */
    UNSUPPORTED_UPDATE_OPERATION_MODIFIER("Unsupported update operation modifier"),
    /** error. */
    UNSUPPORTED_UPDATE_OPERATION_PARAM("Unsupported update operation parameter"),
    /** error. */
    UNSUPPORTED_UPDATE_OPERATION_PATH("Invalid update operation path"),
    /** error. */
    UNSUPPORTED_UPDATE_OPERATION_TARGET("Unsupported target JSON value for update operation"),
    /** error. */
    UNSUPPORTED_UPDATE_FOR_DOC_ID("Cannot use operator with '_id' field"),
    /** error. */
    UNSUPPORTED_UPDATE_FOR_VECTOR("Cannot use operator with '$vector' field"),
    /** error. */
    UNSUPPORTED_UPDATE_FOR_VECTORIZE("Cannot use operator with '$vectorize' field"),

    /** error. */
    VECTOR_SEARCH_NOT_AVAILABLE("Vector search functionality is not available in the backend"),

    /** error. */
    VECTOR_SEARCH_USAGE_ERROR("Vector search can't be used with other sort clause"),

    /** error. */
    VECTOR_SEARCH_NOT_SUPPORTED("Vector search is not enabled for the collection "),

    /** error. */
    VECTOR_SEARCH_INVALID_FUNCTION_NAME("Invalid vector search function name: "),

    /** error. */
    VECTOR_SEARCH_FIELD_TOO_BIG("Vector embedding field '$vector' length too big"),

    /** error. */
    VECTORIZE_SERVICE_NOT_REGISTERED("Vectorize service name provided is not registered : "),

    /** error. */
    VECTORIZE_SERVICE_TYPE_NOT_ENABLED("Vectorize service type not enabled : "),

    /** error. */
    VECTORIZE_SERVICE_TYPE_UNSUPPORTED("Vectorize service type unsupported : "),

    /** error. */
    VECTORIZE_SERVICE_TYPE_UNAVAILABLE("Vectorize service unavailable : "),

    /** error. */
    VECTORIZE_USAGE_ERROR("Vectorize search can't be used with other sort clause"),

    /** error. */
    VECTORIZECONFIG_CHECK_FAIL("Internal server error: VectorizeConfig check fail");

    /** error. */
    private final String message;

    /**
     * Constructor.
     *
     * @param message
     *      error message
     */
    DataApiErrorCode(String message) {
        this.message = message;
    }

    /**
     * Gets message
     *
     * @return value of message
     */
    public String getMessage() {
        return message;
    }
}
