package io.stargate.data_api.client.exception;

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
        super("Cannot find Namespace " + namespace);
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
        super("Cannot find Namespace " + namespace, parent);
    }

}
