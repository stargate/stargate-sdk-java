package io.stargate.sdk.data.client.exception;

import io.stargate.sdk.data.client.model.DataApiCommandExecutionInfos;
import io.stargate.sdk.data.client.model.DataApiError;
import io.stargate.sdk.data.client.model.DataApiResponse;
import io.stargate.sdk.utils.Assert;
import lombok.Getter;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Class to wrap error from the Data API.
 */
@Getter
public class DataApiResponseException extends DataApiException {

    /**
     * Trace the execution results information.
     */
    List<DataApiCommandExecutionInfos> commandsList;

    /**
     * Constructor with list of constructors.
     *
     * @param cmdList
     *      command execution list
     */
    public DataApiResponseException(List<DataApiCommandExecutionInfos> cmdList) {
        super(getErrorCode(cmdList), getErrorMessage(cmdList));
        this.commandsList = cmdList;
    }

    /**
     * Flattening errors as a list.
     *
     * @return
     *      list of errors
     */
    public List<DataApiError> getApiErrors() {
        if (commandsList != null) {
            return commandsList.stream()
                    .map(DataApiCommandExecutionInfos::getResponse)
                    .filter(Objects::nonNull)
                    .map(DataApiResponse::getErrors)
                    .filter(Objects::nonNull)
                    .flatMap(List::stream)
                    .collect(Collectors.toList());
        }
        return new ArrayList<>();
    }

    /**
     * Parse the command list to get first error of first command.
     *
     * @param commands
     *      input command list
     * @return
     *      error message from the API
     */
    public static String getErrorMessage(List<DataApiCommandExecutionInfos> commands) {
        Assert.notNull(commands, "commandList");
        return findFirstError(commands).map(DataApiError::getErrorMessage).orElse(DEFAULT_ERROR_MESSAGE);
    }

    /**
     * Parse the command list to get first error of first command.
     *
     * @param commands
     *      input command list
     * @return
     *      error code from the API
     */
    public static String getErrorCode(List<DataApiCommandExecutionInfos> commands) {
        Assert.notNull(commands, "commandList");
        return findFirstError(commands).map(DataApiError::getErrorCode).orElse(DEFAULT_ERROR_CODE);
    }

    /**
     * Scan the command execution list to return first Error in appearance.
     *
     * @param commands
     *      command list
     * @return
     *      first error if exists
     */
    private static Optional<DataApiError> findFirstError(List<DataApiCommandExecutionInfos> commands) {
        for (DataApiCommandExecutionInfos command :commands) {
            if (command.getResponse() != null
                    && command.getResponse().getErrors()!= null
                    && !command.getResponse().getErrors().isEmpty()) {
                return Optional.ofNullable(command.getResponse().getErrors().get(0));
            }
        }
        return Optional.empty();
    }


}

