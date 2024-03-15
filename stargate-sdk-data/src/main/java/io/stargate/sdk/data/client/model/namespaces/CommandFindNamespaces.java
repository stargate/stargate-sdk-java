package io.stargate.sdk.data.client.model.namespaces;

import io.stargate.sdk.data.client.model.DataApiCommand;

public class CommandFindNamespaces extends DataApiCommand<Object> {

    /**
     * Default FindCollection
     */
    public CommandFindNamespaces() {
        super("findNamespaces");
    }
}
