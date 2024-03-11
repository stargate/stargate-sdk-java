package io.stargate.sdk.data.client.exception;

/**
 * Specialized Error.
 */
public class NamespaceNotFoundException extends RuntimeException {

    /** Serial. */
    private static final long serialVersionUID = -4491748257797687008L;

    /**
     * Constructor with the column name.
     *
     * @param namespace
     *      column name
     */
    public NamespaceNotFoundException(String namespace) {
        super(errorMessage(namespace));
    }

    /**
     * Full constructor.
     *
     * @param namespace
     *      column name
     * @param parent
     *      parent exception
     */
    public NamespaceNotFoundException(String namespace, Throwable parent) {
        super(errorMessage(namespace), parent);
    }

    /**
     * Build the error message.
     *
     * @param namespace
     *      current namespace
     * @return
     *      error message
     */
    private static String errorMessage(String namespace) {
        return "Namespace '" + namespace + "' does not exist";
    }

}
