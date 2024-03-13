package io.stargate.sdk.data.client.model.find;

import io.stargate.sdk.data.client.model.Command;
import io.stargate.sdk.data.client.model.Filter;
import lombok.Data;

import java.util.Map;

/**
 * Represent a findOne command.
 */
public class CommandFindOne extends Command<CommandFindOne.FindOneRequest> {

    /**
     * Specialization of the command.
     */
    public CommandFindOne() {
        super("findOne");
        payload = new FindOneRequest();
    }

    public CommandFindOne withFilter(Filter filter) {
        if (filter != null) {
            payload.setFilter(filter.getFilter());
        }
        return this;
    }

    public CommandFindOne withOptions(FindOneOptions findOneOptions) {
        if (findOneOptions != null) {
            payload.setSort(findOneOptions.getSort());
            payload.setOptions(findOneOptions.getOptions());
            payload.setProjection(findOneOptions.getProjection());
        }
        return this;
    }

    /**
     * Options of the FindOne command.
     */
    @Data
    public static class FindOneCommandOptions {
        Boolean includeSimilarity;
    }

    /**
     * Json Api Query Payload Wrapper.
     */
    @Data
    public static class FindOneRequest {

        /**
         * where clause
         */
        private Map<String, Object> filter;

        /**
         * Order by.
         */
        private Map<String, Object> sort;

        /**
         * Select.
         */
        private Map<String, Integer> projection;

        /**
         * Options.
         */
        private FindOneCommandOptions options;

    }




}
