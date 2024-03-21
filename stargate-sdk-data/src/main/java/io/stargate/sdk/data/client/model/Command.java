package io.stargate.sdk.data.client.model;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import io.stargate.sdk.data.client.model.update.Update;
import lombok.Data;

import java.io.IOException;
import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static io.stargate.sdk.utils.Assert.hasLength;

/**
 * Represent a command to be executed against the Data API.
 */
@Data
@JsonSerialize(using = Command.CommandSerializer.class)
public class Command implements Serializable {

    /** Command Name. */
    protected String name;

    /** Command payload.*/
    protected Document payload = new Document();

    public static Command create(String name) {
        return new Command(name);
    }

    public Command() {}

    public Command(String name) {
        hasLength(name, "command name");
        this.name = name;
    }

    public Command withFilter(Filter filter) {
        payload.appendIfNotNull("filter", filter);
        return this;
    }

    public Command withReplacement(Object replacement) {
        payload.appendIfNotNull("replacement", replacement);
        return this;
    }

    public Command withProjection(Map<String, Integer> projection) {
        payload.appendIfNotNull("projection", projection);
        return this;
    }

    public Command withSort(Document sort) {
        payload.appendIfNotNull("sort", sort);
        return this;
    }

    public Command withOptions(Object options) {
        payload.appendIfNotNull("options", options);
        return this;
    }

    public Command withDocument(Object document) {
        payload.appendIfNotNull("document", document);
        return this;
    }

    public <DOC> Command withDocuments(List<DOC> documents) {
        payload.appendIfNotNull("documents", documents);
        return this;
    }

    public Command append(String key, Object obj) {
        payload.appendIfNotNull(key, obj);
        return this;
    }

    public Command withUpdate(Update update) {
        payload.appendIfNotNull("update", update);
        return this;
    }

    /**
     * Specialization of the command.
     *
     * @param payload
     *      command payload
     */
    public Command(String name, Document payload) {
        this.name = name;
        this.payload = payload;
    }

    /**
     * Custom serializer for Command class.
     */
    public static class CommandSerializer extends StdSerializer<Command> {

        /**
         * Default constructor.
         */
        public CommandSerializer() {
            this(null);
        }

        /**
         * Constructor with the class in used.
         *
         * @param clazz
         *      type of command for serialization
         */
        public CommandSerializer(Class<Command> clazz) {
            super(clazz);
        }

        /** {@inheritDoc} */
        @Override
        public void serialize(Command command, JsonGenerator gen, SerializerProvider provider) throws IOException {
            LinkedHashMap<String, Object> commandMap = new LinkedHashMap<>();
            commandMap.put(command.getName(), command.getPayload());
            gen.writeObject(commandMap);
        }
    }

}
