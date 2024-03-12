package io.stargate.sdk.data.client.model;

import io.stargate.sdk.data.client.model.find.FindOneOptions;
import io.stargate.sdk.data.client.model.find.FindOneRequest;

/**
 * Represent a findOne command.
 */
public class CommandFindOne extends Command<FindOneRequest> {

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



}
