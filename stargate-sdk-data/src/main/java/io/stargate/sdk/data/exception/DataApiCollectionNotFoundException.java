package io.stargate.sdk.data.exception;

/**
 * Specialized Error.
 */
public class DataApiCollectionNotFoundException extends RuntimeException {
    
    /** Serial. */
    private static final long serialVersionUID = -4491748257797687008L;

    /**
     * Constructor with the column name.
     * 
     * @param colName
     *      column name
     */
    public DataApiCollectionNotFoundException(String colName) {
        super("Cannot find Collection " + colName);
    }
    
    /**
     * Full constructor.
     *
     * @param colName
     *      column name
     * @param parent
     *      parent exception
     */
    public DataApiCollectionNotFoundException(String colName, Throwable parent) {
        super("Cannot find Collection " + colName, parent);
    }

}
