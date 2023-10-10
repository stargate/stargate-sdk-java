package io.stargate.test.json;

import dev.langchain4j.model.huggingface.HuggingFaceEmbeddingModel;
import dev.langchain4j.model.openai.OpenAiEmbeddingModel;
import dev.langchain4j.model.openai.OpenAiModelName;
import io.stargate.sdk.core.domain.Page;
import io.stargate.sdk.json.JsonApiClient;
import io.stargate.sdk.json.domain.odm.Record;
import io.stargate.sdk.json.vector.SimilarityMetric;
import io.stargate.sdk.json.vector.VectorStore;
import lombok.AllArgsConstructor;
import lombok.Data;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileNotFoundException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Scanner;
import java.util.Set;

public class VectorClientPhilosopherTest {

    @Data @AllArgsConstructor
    private static class Quote {
        private String philosopher;
        private String quote;
        private Set<String> tags;
    }

    static HuggingFaceEmbeddingModel huggingFaceModel = HuggingFaceEmbeddingModel.builder()
            .accessToken(System.getenv("HF_API_KEY"))
            .modelId("sentence-transformers/all-MiniLM-L6-v2")
            .build();

    public static float[] vectorizeWithHuggingFace(String inputText) {
        return huggingFaceModel.embed(inputText).content().vector();
    }

    static OpenAiEmbeddingModel openAIModel = OpenAiEmbeddingModel.builder()
             .apiKey(System.getenv("OPENAI_API_KEY"))
             .modelName(OpenAiModelName.TEXT_EMBEDDING_ADA_002)
             .timeout(Duration.ofSeconds(15))
             .logRequests(true)
             .logResponses(true)
             .build();

    public static float[] vectorizeWithOpenAi(String inputText) {
        return huggingFaceModel.embed(inputText).content().vector();
    }

    @Test
    public void shouldUseVectorClientLocal() {

        // Initialization
        JsonApiClient jsonApiClient = new JsonApiClient();
        jsonApiClient
                .createNamespace("vector3")
                .createCollectionVector("philosophers", 384, SimilarityMetric.cosine);

        // Initializing vector store with object mapping
        VectorStore<Quote> vectorStore = jsonApiClient
                .namespace("vector3")
                .vectorStore("philosophers", Quote.class);

        // Ingest CSV
        loadQuotesFromCsv("/philosopher-quotes.csv")
                .forEach(quote -> vectorStore.insert(quote, vectorizeWithHuggingFace(quote.getQuote())));

        // Ann Search
        float[] embeddings = huggingFaceModel.embed("I think therefore I am").content().vector();
        Page<Record<Quote>> page = vectorStore.annSearch(embeddings, null, 5);
        page.getResults().forEach(record -> System.out.println(record.getData().getQuote()));

    }

    private List<Quote> loadQuotesFromCsv(String filePath) {
        List<Quote> quotes = new ArrayList<>();
        File csvFile = new File(LoadQuotes.class.getResource(filePath).getFile());
        try (Scanner scanner = new Scanner(csvFile)) {
            while (scanner.hasNextLine()) {
                Quote q = mapCsvLine(scanner.nextLine());
                if (q != null) quotes.add(q);
            }
        } catch (FileNotFoundException fex) {
            throw new IllegalArgumentException("file is not in the classpath", fex);
        }
        return quotes;
    }

    private Quote mapCsvLine(String line) {
        String[] parts = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);
        if (parts.length >= 3) {
            String author    = parts[0];
            String quote     = parts[1].replaceAll("\"", "");
            Set<String> tags = new HashSet<>(Arrays.asList(parts[2].split("\\;")));
            return new Quote(author, quote, tags);
        }
        return null;
    }

}
