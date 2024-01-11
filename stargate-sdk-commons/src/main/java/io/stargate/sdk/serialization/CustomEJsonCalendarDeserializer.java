package io.stargate.sdk.serialization;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;

import java.io.IOException;
import java.util.Calendar;
import java.util.Date;

/**
 * Custom deserializer for EJson Date type.
 */
public class CustomEJsonCalendarDeserializer extends JsonDeserializer<Calendar> {

    /**
     * Default constructor.
     */
    public CustomEJsonCalendarDeserializer() {
    }

    /** {@inheritDoc} */
    @Override
    public Calendar deserialize(JsonParser jp, DeserializationContext ctxt)
    throws IOException, JsonProcessingException {
        JsonNode node = jp.getCodec().readTree(jp);
        long timestamp = node.get("$date").asLong();
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(timestamp);
        return calendar;
    }

}