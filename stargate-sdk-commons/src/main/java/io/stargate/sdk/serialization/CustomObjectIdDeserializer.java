package io.stargate.sdk.serialization;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import io.stargate.sdk.types.ObjectId;

import java.io.IOException;
import java.util.Date;

/**
 * Custom deserializer for EJson Date type.
 */
public class CustomObjectIdDeserializer extends JsonDeserializer<ObjectId> {

    /**
     * Default constructor.
     */
    public CustomObjectIdDeserializer() {
    }

    /** {@inheritDoc} */
    @Override
    public ObjectId deserialize(JsonParser jp, DeserializationContext ctxt)
    throws IOException, JsonProcessingException {
        JsonNode node = jp.getCodec().readTree(jp);
        String hexString = node.get("$objectId").asText();
        return new ObjectId(hexString);
    }

}