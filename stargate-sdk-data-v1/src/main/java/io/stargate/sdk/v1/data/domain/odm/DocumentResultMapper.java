package io.stargate.sdk.v1.data.domain.odm;

import io.stargate.sdk.v1.data.domain.JsonDocumentResult;

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