package io.stargate.sdk.data.client.model.delete;

import io.stargate.sdk.data.client.model.DataApiCommand;
import io.stargate.sdk.data.client.model.Filter;
import lombok.Data;

import java.util.Map;

public class CommandDeleteMany extends DataApiCommand<CommandDeleteMany.Payload> {

    /**
     * Specialization of the command.
     */
    public CommandDeleteMany() {
        super("deleteMany");
        payload = new CommandDeleteMany.Payload();
    }

    public CommandDeleteMany withFilter(Filter filter) {
        if (filter != null) {
            payload.setFilter(filter.getFilter());
        }
        return this;
    }

    /**
     * Json Api Query Payload Wrapper.
     */
    @Data
    public static class Payload {
        /**
         * where clause
         */
        private Map<String, Object> filter;

    }
}
