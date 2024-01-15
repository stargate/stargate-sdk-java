package io.stargate.sdk.data.domain.odm;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import io.stargate.sdk.utils.JsonUtils;

import java.io.IOException;
import java.util.Map;

/**
 * Custom serializer for Document.
 *
 * @param <T>
 *     Current object at work with the document
 *     (can be a Map or a POJO)
 */
public class DocumentSerializer<T> extends JsonSerializer<Document<T>> {

    /**
     * Default constructor.
     */
    public DocumentSerializer() {}

    /** {@inheritDoc} */
    @Override
    @SuppressWarnings("unchecked")
    public void serialize(Document<T> value, JsonGenerator gen, SerializerProvider serializerProvider) throws IOException {
        gen.writeStartObject();
        Map<String, Object> dataMap = null;
        if (value.getData() instanceof Map) {
            dataMap = (Map<String, Object>) value.getData();
        } else {
            dataMap = JsonUtils.convertValueForDataApi(value.getData(), Map.class);
        }
        for (Map.Entry<String, Object> entry : dataMap.entrySet()) {
            gen.writeObjectField(entry.getKey(), entry.getValue());
        }
        if (!dataMap.containsKey("_id") && value.getId() != null) {
            gen.writeStringField("_id", value.getId());
        }
        if (!dataMap.containsKey("$vector") && value.getVector() != null) {
            gen.writeArrayFieldStart("$vector");
            for (float v : value.getVector()) {
                gen.writeNumber(v);
            }
            gen.writeEndArray();
        }
        gen.writeEndObject();
    }
}