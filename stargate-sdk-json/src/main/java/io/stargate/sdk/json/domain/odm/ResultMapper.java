package io.stargate.sdk.json.domain.odm;

import io.stargate.sdk.json.domain.JsonResult;

/**
 * Extension point for the user to implement its own parser for a record.
 *
 * @param <T>
 *      working bean
 */
@FunctionalInterface
public interface ResultMapper<T> {

    /**
     * Extension point for the user to implement its own parser for a record.
     *
     * @param record
     *      current record
     * @return
     *      the object marshalled
     */
    Result<T> map(JsonResult record);

}