package io.stargate.sdk.data.client.model;

import io.stargate.sdk.data.internal.model.NamespaceInformation;

/**
 * Create namespace command.
 */
public class CommandCreateNamespace extends Command<NamespaceInformation> {

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
