package io.stargate.sdk.data.test.unit;

import io.stargate.sdk.data.client.model.Document;
import org.junit.jupiter.api.Test;

/**
 * Test different use case of client serialization.
 */
public class DocumentSerializationTest {



    @Test
    public void shouldSerializeAsJson() {
        System.out.println(new Document().append("hello", "world").toJson());

        String json = "{\"hello\":\"world\"}";
        Document doc1 = Document.parse(json);
        System.out.println(doc1.getString("hello"));
    }
}
