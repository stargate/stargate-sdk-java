package io.stargate.sdk.data.client.model.find;

import io.stargate.sdk.data.client.model.DataApiCommand;
import io.stargate.sdk.data.client.model.Filter;
import lombok.Data;

import java.util.Map;

/**
 * Represent a findOne command.
 */
public class CommandFindOne extends DataApiCommand<CommandFindOne.Payload> {

    /**
     * Specialization of the command.
     */
    public CommandFindOne() {
        super("findOne");
        payload = new Payload();
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
            Options options = new Options();
            options.setIncludeSimilarity(findOneOptions.getIncludeSimilarity());
            payload.setOptions(options);
            payload.setProjection(findOneOptions.getProjection());
        }
        return this;
    }

    /**
     * Options of the FindOne command.
     */
    @Data
    public static class Options {
        Boolean includeSimilarity;
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

        /**
         * Select.
         */
        private Map<String, Integer> projection;

        /**
         * Options.
         */
        private Options options;

    }




}
