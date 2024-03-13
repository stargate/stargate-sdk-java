package io.stargate.sdk.data.test.unit;

import io.stargate.sdk.data.client.model.collections.CommandCreateCollection;
import io.stargate.sdk.data.client.model.collections.CreateCollectionOptions;
import io.stargate.sdk.data.client.model.Document;
import io.stargate.sdk.data.client.model.SimilarityMetric;
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
        CommandCreateCollection ccc = new CommandCreateCollection()
                .withName("demo")
                .withOptions(CreateCollectionOptions.builder()
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
