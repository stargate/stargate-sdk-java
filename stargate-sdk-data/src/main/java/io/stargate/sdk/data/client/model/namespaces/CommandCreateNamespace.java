package io.stargate.sdk.data.client.model.namespaces;

import io.stargate.sdk.data.client.model.DataApiCommand;

/**
 * Create namespace command.
 */
public class CommandCreateNamespace extends DataApiCommand<NamespaceInformation> {

    /**
     * Specialization of the command.
     */
    public CommandCreateNamespace() {
        super("createNamespace");
        payload = new NamespaceInformation();
    }

    /**
     * Specialization of the command.
     *
     * @param name
     *      name of the namespace
     */
    public CommandCreateNamespace(String name) {
        this();
        payload.setName(name);
    }

    public CommandCreateNamespace withName(String name) {
        payload.setName(name);
        return this;
    }

    public CommandCreateNamespace withOptions(CreateNamespaceOptions options) {
        payload.setOptions(options);
        return this;
    }

}
