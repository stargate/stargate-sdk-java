package io.stargate.sdk.data.domain.odm;

import io.stargate.sdk.data.domain.JsonDocumentResult;

/**
 * Extension point for the user to implement its own parser for a record.
 *
 * @param <T>
 *      working bean
 */
@FunctionalInterface
public interface DocumentResultMapper<T> {

    /**
     * Extension point for the user to implement its own parser for a record.
     *
     * @param record
     *      current record
     * @return
     *      the object marshalled
     */
    DocumentResult<T> map(JsonDocumentResult record);

}