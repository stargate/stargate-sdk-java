package io.stargate.sdk.data.client.model;

import io.stargate.sdk.utils.Assert;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.Map;

public class CommandInsertOne<DOC> extends Command<CommandInsertOne.Payload<DOC>>  {

    /**
     * Default FindCollection
     */
    public CommandInsertOne(DOC doc) {
        super("insertOne");
        Assert.notNull(doc, "document");
        this.payload = new Payload<DOC>(doc);
    }

    @Data @AllArgsConstructor
    public static class Payload<DOC> {
        DOC document;
    }

}
