package io.stargate.sdk.data.client.model.delete;

import io.stargate.sdk.data.client.model.DataApiCommand;
import io.stargate.sdk.data.client.model.Filter;
import io.stargate.sdk.data.client.model.find.CommandFindOne;
import io.stargate.sdk.data.client.model.find.FindOneOptions;
import lombok.Data;

import java.util.Map;

public class CommandDeleteOne extends DataApiCommand<CommandDeleteOne.Payload> {

    /**
     * Specialization of the command.
     */
    public CommandDeleteOne() {
        super("deleteOne");
        payload = new CommandDeleteOne.Payload();
    }

    public CommandDeleteOne withFilter(Filter filter) {
        if (filter != null) {
            payload.setFilter(filter.getFilter());
        }
        return this;
    }

    public CommandDeleteOne withOptions(DeleteOneOptions deleteOneOptions) {
        if (deleteOneOptions != null) {
            payload.setSort(deleteOneOptions.getSort());
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

        /**
         * Order by.
         */
        private Map<String, Object> sort;

    }

}
