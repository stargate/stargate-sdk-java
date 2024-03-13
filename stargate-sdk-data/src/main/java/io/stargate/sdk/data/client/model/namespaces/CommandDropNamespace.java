package io.stargate.sdk.data.client.model.namespaces;

import io.stargate.sdk.data.client.model.Command;
import io.stargate.sdk.utils.Assert;
import lombok.AllArgsConstructor;
import lombok.Data;

public class CommandDropNamespace extends Command<CommandDropNamespace.Payload> {

    /**
     * Default FindCollection
     */
    public CommandDropNamespace(String name) {
        super("dropNamespace");
        Assert.hasLength(name, "name");
        this.payload = new Payload(name);
    }

    @Data
    @AllArgsConstructor
    public static class Payload {
        String name;
    }


}
