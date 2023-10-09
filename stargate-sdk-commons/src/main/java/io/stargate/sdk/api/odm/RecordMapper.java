package io.stargate.sdk.api.odm;

/**
 * Extension point for the user to implement its own parser for a record.
 *
 * @author Cedrick LUNVEN (@clunven)
 *
 * @param <DOC>
 *      working bean
 */
@FunctionalInterface
public interface RecordMapper<DOC> {
    
    /**
     * Extension point for the user to implement its own parser for a record.
     * 
     * @param record
     *      current record
     * @return
     *      the object marshalled
     */
    DOC map(String record);

}
