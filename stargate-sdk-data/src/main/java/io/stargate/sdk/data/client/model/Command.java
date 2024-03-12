package io.stargate.sdk.data.client.model;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import lombok.Data;

import java.io.IOException;
import java.io.Serializable;
import java.util.LinkedHashMap;

import static io.stargate.sdk.utils.Assert.hasLength;

/**
 * Represent a command to be executed against the Data API.
 */
@Data
@JsonSerialize(using = Command.CommandSerializer.class)
public class Command<T> implements Serializable {

    /** Command Name. */
    String name;

    /** Command payload.*/
    T payload;

    public Command(String name) {
        hasLength(name, "command name");
        this.name = name;
    }

    /**
     * Specialization of the command.
     *
     * @param payload
     *      command payload
     */
    public Command(String name, T payload) {
        this.name = name;
        this.payload = payload;
    }

    /**
     * Custom serializer for Command class.
     */
    public static class CommandSerializer extends StdSerializer<Command<?>> {

        public CommandSerializer() {
            this(null);
        }

        public CommandSerializer(Class<Command<?>> t) {
            super(t);
        }

        @Override
        public void serialize(Command<?> command, JsonGenerator gen, SerializerProvider provider) throws IOException {
            LinkedHashMap<String, Object> commandMap = new LinkedHashMap<>();
            commandMap.put(command.getName(), command.getPayload() == null ?  new Object() : command.getPayload());
            gen.writeObject(commandMap);
        }
    }

}
