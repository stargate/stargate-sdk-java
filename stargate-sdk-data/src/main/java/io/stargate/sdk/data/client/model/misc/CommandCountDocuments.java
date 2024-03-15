package io.stargate.sdk.data.client.model.misc;

import io.stargate.sdk.data.client.model.DataApiCommand;
import io.stargate.sdk.data.client.model.Filter;

/**
 * Command to count Documents in a collection.
 */
public class CommandCountDocuments extends DataApiCommand<Filter> {

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
