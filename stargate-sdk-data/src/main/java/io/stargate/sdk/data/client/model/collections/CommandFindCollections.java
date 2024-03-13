package io.stargate.sdk.data.client.model.collections;

import io.stargate.sdk.data.client.model.Command;
import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * Allow to list collections.
 */
public class CommandFindCollections extends Command<CommandFindCollections.Payload> {

    /**
     * Default FindCollection
     */
    public CommandFindCollections() {
        super("findCollections");
        payload = new Payload();
    }

    /**
     * Add options.
     *
     * @return
     *      collection with explain
     */
    public CommandFindCollections withExplain(Boolean bool) {
        payload.setOptions(new FindCollectionsOptions(bool));
        return this;
    }

    /**
     * Options for a FindCollection.
     */
    @Data
    public static class Payload {
        FindCollectionsOptions options;
    }

    /**
     * Options for a FindCollection.
     */
    @Data @AllArgsConstructor
    public static class FindCollectionsOptions {
        Boolean explain = true;
    }


}
