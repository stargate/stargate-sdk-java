package io.stargate.sdk.data.client.model.collections;

import io.stargate.sdk.data.client.model.Command;
import io.stargate.sdk.utils.Assert;
import lombok.AllArgsConstructor;
import lombok.Data;

public class CommandDropCollection extends Command<CommandDropCollection.Payload> {

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
