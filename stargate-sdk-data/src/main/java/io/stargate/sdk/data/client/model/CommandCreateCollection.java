package io.stargate.sdk.data.client.model;

import io.stargate.sdk.data.internal.model.CollectionDefinition;
import io.stargate.sdk.utils.Assert;

import java.util.Map;

/**
 * Representation of a createCollection Command.
 */
public class CommandCreateCollection extends Command<CollectionDefinition>  {

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
    public CommandCreateCollection withOptions(CreateCollectionOptions options) {
        payload.setOptions(options);
        return this;
    }
}
