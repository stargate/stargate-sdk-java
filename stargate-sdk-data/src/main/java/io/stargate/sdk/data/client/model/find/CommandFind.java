package io.stargate.sdk.data.client.model.find;

import io.stargate.sdk.data.client.model.DataApiCommand;
import io.stargate.sdk.data.client.model.Filter;
import lombok.Data;

import java.util.Map;

public class CommandFind extends DataApiCommand<CommandFind.Payload> {

    /**
     * Specialization of the command.
     */
    public CommandFind() {
        super("find");
        payload = new Payload();
    }

    public CommandFind withFilter(Filter filter) {
        if (filter != null) {
            payload.setFilter(filter.getFilter());
        }
        return this;
    }

    public CommandFind withOptions(FindOptions findOptions) {
        if (findOptions != null) {
            // Sort and Project
            payload.setSort(findOptions.getSort());
            payload.setProjection(findOptions.getProjection());

            // Options of a find mapped
            CommandFind.Options options = new CommandFind.Options();
            options.skip = findOptions.getSkip();
            options.limit = findOptions.getLimit();
            options.pageState = findOptions.getPageState();
            options.includeSimilarity = findOptions.getIncludeSimilarity();
            payload.setOptions(options);
        }
        return this;
    }

    /**
     * Mutation of the pageState.
     *
     * @param pageState
     *      current pageState
     * @return
     *      current reference
     */
    public CommandFind withPageState(String pageState) {
        payload.getOptions().pageState = pageState;
        return this;
    }

    /**
     * Options of the Find command.
     */
    @Data
    public static class Options {
        Integer skip;

        Integer limit;

        Boolean includeSimilarity;

        String pageState;
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
