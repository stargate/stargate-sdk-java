package io.stargate.sdk.serialization;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import io.stargate.sdk.types.UUIDv6;

import java.io.IOException;
import java.util.UUID;

/**
 * Object Id Could be
 * objectId|uuid|uuidv6|uuidv7
 */
public class CustomUuidv6Serializer extends StdSerializer<UUIDv6> {
    /**
     * Default constructor.
     */
    public CustomUuidv6Serializer() {
        this(null);
    }

    /**
     * Constructor with type
     * @param t
     *      type
     */
    public CustomUuidv6Serializer(Class<UUIDv6> t) {
        super(t);
    }

    /** {@inheritDoc} */
    @Override
    public void serialize(UUIDv6 uuidv6, JsonGenerator gen, SerializerProvider provider) throws IOException {
        gen.writeStartObject();
        gen.writeStringField("$uuidv6", uuidv6.toString());
        gen.writeEndObject();
    }
}
