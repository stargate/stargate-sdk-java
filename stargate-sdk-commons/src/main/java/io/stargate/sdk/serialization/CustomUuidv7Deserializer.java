package io.stargate.sdk.serialization;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import io.stargate.sdk.types.UUIDv6;
import io.stargate.sdk.types.UUIDv7;

import java.io.IOException;

/**
 * Custom deserializer for EJson Date type.
 */
public class CustomUuidv7Deserializer extends JsonDeserializer<UUIDv7> {

    /**
     * Default constructor.
     */
    public CustomUuidv7Deserializer() {
    }

    /** {@inheritDoc} */
    @Override
    public UUIDv7 deserialize(JsonParser jp, DeserializationContext ctxt)
    throws IOException, JsonProcessingException {
        JsonNode node = jp.getCodec().readTree(jp);
        String hexString = node.get("$uuidv7").asText();
        return UUIDv7.fromString(hexString);
    }

}