package io.stargate.sdk.data.client.model.collections;

import io.stargate.sdk.data.client.model.DataApiCommand;
import io.stargate.sdk.utils.Assert;
import lombok.AllArgsConstructor;
import lombok.Data;

public class CommandDropCollection extends DataApiCommand<CommandDropCollection.Payload> {

    /**
     * Default FindCollection
     */
    public CommandDropCollection(String name) {
        super("deleteCollection");
        Assert.hasLength(name, "name");
        this.payload = new Payload(name);
    }

    @Data
    @AllArgsConstructor
    public static class Payload {
        String name;
    }

}
