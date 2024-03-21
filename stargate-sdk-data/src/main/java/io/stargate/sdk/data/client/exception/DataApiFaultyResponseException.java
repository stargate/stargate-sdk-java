package io.stargate.sdk.data.client.exception;

import io.stargate.sdk.data.client.model.Command;
import io.stargate.sdk.data.client.model.ApiResponse;

/**
 * Error when API reply unexpected results
 */
public class DataApiFaultyResponseException extends DataApiException {

    /** Command which trigger the error. */
    private final Command command;

    /** Data Api response for the error. */
    private final ApiResponse response;

    /**
     * Constructor with command and respond.
     * @param cmd
     *      command
     * @param res
     *      response from the API
     * @param msg
     *      error nmessage
     */
    public DataApiFaultyResponseException(Command cmd, ApiResponse res, String msg) {
        super(DEFAULT_ERROR_CODE, msg);
        this.command  = cmd;
        this.response = res;

    }
}
