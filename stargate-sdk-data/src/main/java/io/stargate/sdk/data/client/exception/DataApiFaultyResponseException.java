package io.stargate.sdk.data.client.exception;

import io.stargate.sdk.data.client.model.DataApiCommand;
import io.stargate.sdk.data.client.model.DataApiResponse;

/**
 * Error when API reply unexpected results
 */
public class DataApiFaultyResponseException extends DataApiException {

    /** Command which trigger the error. */
    private final DataApiCommand<?> command;

    /** Data Api response for the error. */
    private final DataApiResponse response;

    /**
     * Constructor with command and respond.
     * @param cmd
     *      command
     * @param res
     *      response from the API
     * @param msg
     *      error nmessage
     */
    public DataApiFaultyResponseException(DataApiCommand<?> cmd, DataApiResponse res, String msg) {
        super(DEFAULT_ERROR_CODE, msg);
        this.command  = cmd;
        this.response = res;

    }
}
