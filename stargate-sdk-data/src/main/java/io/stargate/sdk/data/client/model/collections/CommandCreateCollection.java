package io.stargate.sdk.data.client.model.collections;

import io.stargate.sdk.data.client.model.DataApiCommand;
import io.stargate.sdk.utils.Assert;

/**
 * Representation of a createCollection Command.
 */
public class CommandCreateCollection extends DataApiCommand<CollectionDefinition> {

    /**
     * Specialization of the command.
     */
    public CommandCreateCollection() {
        super("createCollection");
        payload = new CollectionDefinition();
    }

    /**
     * Specialization of the command.
     *
     * @param name
     *      collection Name
     */
    public CommandCreateCollection(String name) {
        this();
        payload.setName(name);
    }

    /**
     * Add a name in a collection.
     *
     * @param name
     *      collection name
     * @return
     *      current object
     */
    public CommandCreateCollection withName(String name) {
        Assert.hasLength(name, "name");
        payload.setName(name);
        return this;
    }

    /**
     * Add options in a collection.
     *
     * @param options
     *      collection options
     * @return
     *      current object
     */
    public CommandCreateCollection withOptions(CollectionOptions options) {
        payload.setOptions(options);
        return this;
    }
}
