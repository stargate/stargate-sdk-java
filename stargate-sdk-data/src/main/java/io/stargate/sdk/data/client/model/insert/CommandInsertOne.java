package io.stargate.sdk.data.client.model.insert;

import io.stargate.sdk.data.client.model.DataApiCommand;
import io.stargate.sdk.utils.Assert;
import lombok.AllArgsConstructor;
import lombok.Data;

public class CommandInsertOne<DOC> extends DataApiCommand<CommandInsertOne.Payload<DOC>> {

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
