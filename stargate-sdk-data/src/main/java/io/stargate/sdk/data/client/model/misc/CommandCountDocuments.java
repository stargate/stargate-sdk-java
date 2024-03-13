package io.stargate.sdk.data.client.model.misc;

import io.stargate.sdk.data.client.model.Command;
import io.stargate.sdk.data.client.model.Filter;
import lombok.Data;

/**
 * Command to count Documents in a collection.
 */
public class CommandCountDocuments extends Command<Filter> {

    /**
     * Specialization of the command.
     */
    public CommandCountDocuments() {
        super("countDocuments");
    }

    /**
     * Create a command with the filter.
     *
     * @param filter
     *      count filter
     */
    public CommandCountDocuments(Filter filter) {
        this();
        this.payload = filter;
    }

}
