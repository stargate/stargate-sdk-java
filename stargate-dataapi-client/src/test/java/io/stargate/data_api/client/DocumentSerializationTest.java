package io.stargate.data_api.client;

import io.stargate.data_api.client.model.Document;
import org.junit.jupiter.api.Test;

import javax.print.Doc;

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
