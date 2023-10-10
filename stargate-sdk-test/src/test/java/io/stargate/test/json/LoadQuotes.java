package io.stargate.test.json;

import dev.langchain4j.data.embedding.Embedding;
import dev.langchain4j.data.message.AiMessage;
import dev.langchain4j.model.huggingface.HuggingFaceChatModel;
import dev.langchain4j.model.huggingface.HuggingFaceEmbeddingModel;
import lombok.AllArgsConstructor;
import lombok.Data;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Scanner;
import java.util.Set;
import static dev.langchain4j.model.huggingface.HuggingFaceModelName.TII_UAE_FALCON_7B_INSTRUCT;
import static java.time.Duration.ofSeconds;
import static dev.langchain4j.data.message.SystemMessage.systemMessage;
import static dev.langchain4j.data.message.UserMessage.userMessage;

public class LoadQuotes {

    @Data @AllArgsConstructor
    private static class Quote {
        private String philosopher;
        private String quote;
        private Set<String> tags;
    }

    public List<Quote> loadQuotes(String filePath) {
        List<Quote> quotes = new ArrayList<>();
        File csvFile = new File(LoadQuotes.class.getResource(filePath).getFile());
        try (Scanner scanner = new Scanner(csvFile)) {
            while (scanner.hasNextLine()) {
                quotes.add(mapCsvLine(scanner.nextLine()));
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

    @Test
    public void shouldLoadCsv() throws IOException {
        loadQuotes("/philosopher-quotes.csv")
                .forEach(q -> System.out.println(q.getPhilosopher() + " - " + q.getQuote() + " - " + q.getTags()));
    }

    @Test
    public void embeddingHuggingFace() {
        HuggingFaceEmbeddingModel model = HuggingFaceEmbeddingModel.builder()
                .accessToken(System.getenv("HF_API_KEY"))
                .modelId("sentence-transformers/all-MiniLM-L6-v2")
                .build();
        Embedding embedding = model.embed("This is a demo embeddings").content();
        System.out.println(embedding.dimensions());
        System.out.println(Arrays.toString(embedding.vector()));
    }

    @Test
    public void generativeAiLlama2() {
        HuggingFaceChatModel model = HuggingFaceChatModel.builder()
                .accessToken(System.getenv("HF_API_KEY"))
                .modelId(TII_UAE_FALCON_7B_INSTRUCT)
                .timeout(ofSeconds(15))
                .temperature(0.7)
                .maxNewTokens(20)
                .waitForModel(true)
                .build();

        AiMessage aiMessage = model.generate(
                systemMessage("You are a good friend of mine, who likes to answer with jokes"),
                userMessage("Hey Bro, what are you doing?")
        ).content();
        System.out.println(aiMessage.text());
    }


}
