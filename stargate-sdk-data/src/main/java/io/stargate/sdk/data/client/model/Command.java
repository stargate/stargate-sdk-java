package io.stargate.sdk.data.client.model;

/**
 * Represent a command to be executed against the Data API.
 */
public class Command<T> {

    /**
     * Name of the command.
     */
    String name;

    /**
     * Payload for this command
     */
    T payload;

}
