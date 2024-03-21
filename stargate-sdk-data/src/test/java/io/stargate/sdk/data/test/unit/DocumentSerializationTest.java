package io.stargate.sdk.data.test.unit;

import io.stargate.sdk.data.client.model.Command;
import io.stargate.sdk.data.client.model.Document;
import io.stargate.sdk.data.client.model.SimilarityMetric;
import io.stargate.sdk.data.client.model.collections.CollectionOptions;
import io.stargate.sdk.utils.JsonUtils;
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

    @Test
    public void shouldSerializeCommand() {
        Command ccc = Command.create("createCollection")
                .append("name", "demo")
                .withOptions(CollectionOptions.builder()
                    .withVectorDimension(14)
                    .withVectorSimilarityMetric(SimilarityMetric.cosine)
                    .build());
        System.out.println(JsonUtils.marshallForDataApi(ccc));
    }

    @Test
    public void shouldSerializeCommand2() {
        System.out.println(JsonUtils.marshallForDataApi(new Object()));
    }
}
