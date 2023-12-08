package io.stargate.test.json;

import dev.langchain4j.data.message.AiMessage;
import dev.langchain4j.model.chat.ChatLanguageModel;
import dev.langchain4j.model.input.Prompt;
import dev.langchain4j.model.input.PromptTemplate;
import dev.langchain4j.model.openai.OpenAiChatModel;
import dev.langchain4j.model.openai.OpenAiEmbeddingModel;
import dev.langchain4j.model.openai.OpenAiModelName;
import dev.langchain4j.model.output.Response;
import io.stargate.sdk.json.ApiClient;
import io.stargate.sdk.json.CollectionRepository;
import io.stargate.sdk.json.NamespaceClient;
import io.stargate.sdk.json.domain.Filter;
import io.stargate.sdk.json.domain.odm.Document;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import java.io.File;
import java.io.FileNotFoundException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * Philosopher Demo with Vector Client.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class VectorClientPhilosopherTest {

    @Data @AllArgsConstructor @NoArgsConstructor
    private static class Quote {
        private String philosopher;
        private String quote;
        private Set<String> tags;
    }

    // OpenAI Usual Suspects

    static OpenAiEmbeddingModel openaiVectorizer = OpenAiEmbeddingModel.builder()
             .apiKey(System.getenv("OPENAI_API_KEY"))
             .modelName(OpenAiModelName.TEXT_EMBEDDING_ADA_002)
             .timeout(Duration.ofSeconds(15))
             .logRequests(true)
             .logResponses(true)
             .build();

    public static float[] vectorize(String inputText) {
        return openaiVectorizer.embed(inputText).content().vector();
    }

    public static CollectionRepository<Quote> vectorStore;

    @Test
    @Order(1)
    @DisplayName("01. Create a namespace and a collection")
    public void init() {

        // Initialization
        ApiClient jsonApiClient = new ApiClient();
        NamespaceClient nsClient = jsonApiClient.createNamespace("vector_openai");
        nsClient.deleteCollection("philosophers");
        nsClient.createCollection("philosophers", 1536);

       // Low level client
       jsonApiClient.namespace("vector_openai").collection("philosophers");

       // Crud Repository on a Collection
       jsonApiClient.namespace("vector_openai").collectionRepository("philosophers", Quote.class);

       // Vector = crud repository + vector native
       vectorStore = jsonApiClient
                .namespace("vector_openai")
                .collectionRepository("philosophers", Quote.class);
    }


    @Test
    @Order(2)
    @DisplayName("02. Loading DataSet")
    public void shouldLoadDataset() {
        // Ingest CSV
        AtomicInteger rowId = new AtomicInteger();
        loadQuotesFromCsv("/philosopher-quotes.csv").forEach(quote -> {
            System.out.println("Inserting " + rowId.get() + ") " + quote.getQuote());
            vectorStore.insert(new Document<Quote>(
                    String.valueOf(rowId.incrementAndGet()),
                    quote, vectorize(quote.getQuote())));
        });
    }

    @Test
    @Order(3)
    @DisplayName("03. Should Similarity Search")
    public void shouldSimilaritySearch() {

        vectorStore = new ApiClient()
                .namespace("vector_openai")
                .collectionRepository("philosophers", Quote.class);

        float[] embeddings = vectorize("We struggle all our life for nothing");
        vectorStore.findVector(embeddings, null,3)
                .stream()
                .map(Document::getData)
                .map(Quote::getQuote)
                .forEach(System.out::println);
    }

    @Test
    @Order(4)
    @DisplayName("04. Should filter with meta data")
    public void shouldMetaDataFiltering() {

        new ApiClient()
                .namespace("vector_openai")
                .collectionRepository("philosophers", Quote.class)
                .findVector(
                        vectorize("We struggle all our life for nothing"),
                        new Filter().where("philosopher").isEqualsTo("plato"),
                        2)
                .forEach(r -> System.out.println(r.getSimilarity() + " - " + r.getData().getQuote()));
    }

    @Test
    @Order(5)
    @DisplayName("05. Should filter with meta data tags")
    public void shouldMetaDataFilteringWithTags() {
        vectorStore = new ApiClient()
                .namespace("vector_openai")
                .collectionRepository("philosophers", Quote.class);
        vectorStore.count(new Filter().where("tags").isAnArrayContaining("love"));
    }

    static ChatLanguageModel openaiGenerator = OpenAiChatModel.builder()
            .apiKey(System.getenv("OPENAI_API_KEY"))
            .modelName(OpenAiModelName.GPT_3_5_TURBO)
            .temperature(0.7)
            .timeout(Duration.ofSeconds(15))
            .maxRetries(3)
            .logResponses(true)
            .logRequests(true)
            .build();

    @Test
    @Order(6)
    @DisplayName("06. Should Generate new quotes")
    public void should_generate_new_quotes() {
        vectorStore = new ApiClient()
                .namespace("vector_openai")
                .collectionRepository("philosophers", Quote.class);

        // === Params ==
        String topic  = "politics and virtue";
        String author = "nietzsche";
        int records   = 4;

        // ==== RAG ===
        List<String> ragQuotes = vectorStore
                .findVector(
                        vectorize(topic),
                        new Filter().where("philosopher").isEqualsTo(author),2)
                .stream()
                .map(r -> r.getData().getQuote())
                .collect(Collectors.toList());

        // === Completion ===
        PromptTemplate promptTemplate = PromptTemplate.from(
                "Generate a single short philosophical quote on the given topic,\n" +
                        "similar in spirit and form to the provided actual example quotes.\n" +
                        "Do not exceed 20-30 words in your quote.\n" +
                        "REFERENCE TOPIC: \n{{topic}}" +
                        "\nACTUAL EXAMPLES:\n{{rag}}");
        Map<String, Object> variables = new HashMap<>();
        variables.put("topic", topic);
        variables.put("information", String.join(", ", ragQuotes));
        Prompt prompt = promptTemplate.apply(variables);
        Response<AiMessage> aiMessage = openaiGenerator.generate(prompt.toUserMessage());
        String answer = aiMessage.content().text();
        System.out.println(answer);
    }

    // --- Utilities (loading CSV) ---

    private List<Quote> loadQuotesFromCsv(String filePath) {
        List<Quote> quotes = new ArrayList<>();
        File csvFile = new File(VectorClientPhilosopherTest.class.getResource(filePath).getFile());
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
