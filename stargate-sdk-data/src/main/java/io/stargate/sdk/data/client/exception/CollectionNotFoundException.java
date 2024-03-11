package io.stargate.sdk.data.client.exception;

/**
 * Specialized Error.
 */
public class CollectionNotFoundException extends RuntimeException {

    /** Serial. */
    private static final long serialVersionUID = -4491748257797687008L;

    /**
     * Constructor with the column name.
     *
     * @param namespace
     *      column name
     */
    public CollectionNotFoundException(String namespace) {
        super(errorMessage(namespace));
    }

    /**
     * Full constructor.
     *
     * @param collectionName
     *      column name
     * @param parent
     *      parent exception
     */
    public CollectionNotFoundException(String collectionName, Throwable parent) {
        super(errorMessage(collectionName), parent);
    }

    /**
     * Build the error message.
     *
     * @param collectionName
     *      current collectionName
     * @return
     *      error message
     */
    private static String errorMessage(String collectionName) {
        return "Collection '" + collectionName + "' does not exist";
    }

}
