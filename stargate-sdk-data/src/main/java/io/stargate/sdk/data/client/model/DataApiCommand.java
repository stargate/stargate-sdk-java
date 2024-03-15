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
@JsonSerialize(using = DataApiCommand.CommandSerializer.class)
public class DataApiCommand<T> implements Serializable {

    /** Command Name. */
    protected String name;

    /** Command payload.*/
    protected T payload;

    public DataApiCommand(String name) {
        hasLength(name, "command name");
        this.name = name;
    }

    /**
     * Specialization of the command.
     *
     * @param payload
     *      command payload
     */
    public DataApiCommand(String name, T payload) {
        this.name = name;
        this.payload = payload;
    }

    /**
     * Custom serializer for Command class.
     */
    public static class CommandSerializer extends StdSerializer<DataApiCommand<?>> {

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
        public CommandSerializer(Class<DataApiCommand<?>> clazz) {
            super(clazz);
        }

        /** {@inheritDoc} */
        @Override
        public void serialize(DataApiCommand<?> dataApiCommand, JsonGenerator gen, SerializerProvider provider) throws IOException {
            LinkedHashMap<String, Object> commandMap = new LinkedHashMap<>();
            commandMap.put(dataApiCommand.getName(), dataApiCommand.getPayload() == null ?  new Object() : dataApiCommand.getPayload());
            gen.writeObject(commandMap);
        }
    }

}
