package com.summarizerwork.service.service;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
public class GeminiService {

    @Value("${gemini.api.key}")
    private String apiKey;

    private final HttpClient httpClient = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(20))
            .build();

    private final ObjectMapper objectMapper = new ObjectMapper();

    public String summarize(String content) {
        String defaultPrompt = "Please provide a clear and concise summary of the following content. " +
                               "Focus on the main points and ensure the summary is complete with a proper ending sentence:\n\n"
                               + content;

        return executeWithFailover(defaultPrompt);
    }

    private String executeWithFailover(String finalPrompt) {
        List<String> models = getAvailableModels();

        if (models.isEmpty()) {
            log.error("No models found for the provided API Key.");
            throw new RuntimeException("Gemini API Error: No models available for this key.");
        }

        for (String modelName : models) {
            try {
                log.info("Attempting request with model: {}", modelName);
                String result = callGeminiApi(finalPrompt, modelName);
                log.info("Model {} returned a complete response.", modelName);
                return result;
            } catch (TruncatedResponseException e) {
                // Response was cut off — skip this model and try the next one
                log.warn("Model {} returned a truncated response. Trying next model...", modelName);
            } catch (Exception e) {
                log.warn("Model {} failed: {}. Trying next...", modelName, e.getMessage());
            }
        }

        throw new RuntimeException("Fatal: All available Gemini models failed to return a complete response.");
    }

    private List<String> getAvailableModels() {
        List<String> modelList = new ArrayList<>();
        try {
            String url = "https://generativelanguage.googleapis.com/v1beta/models?key=" + apiKey;

            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .GET()
                    .build();

            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

            if (response.statusCode() != 200) {
                log.error("Failed to list models. Status: {}, Body: {}", response.statusCode(), response.body());
                return modelList;
            }

            JsonNode root = objectMapper.readTree(response.body());
            JsonNode modelsNode = root.path("models");

            for (JsonNode m : modelsNode) {
                String name = m.path("name").asText(); // e.g. "models/gemini-1.5-flash"

                // Only include models that support text generation
                boolean supportsGenerate = false;
                for (JsonNode method : m.path("supportedGenerationMethods")) {
                    if ("generateContent".equals(method.asText())) {
                        supportsGenerate = true;
                        break;
                    }
                }

                if (supportsGenerate) {
                    modelList.add(name);
                }
            }

            // Prioritize 'flash' models (faster/cheaper) first
            modelList.sort(Comparator.comparing(name -> !name.toLowerCase().contains("flash")));

            log.info("Discovered {} usable Gemini models.", modelList.size());

        } catch (Exception e) {
            log.error("Critical error fetching Gemini model list: {}", e.getMessage());
        }
        return modelList;
    }

     //Performs the actual HTTP POST to the Gemini API.
    private String callGeminiApi(String prompt, String modelName) throws Exception {
        String url = "https://generativelanguage.googleapis.com/v1beta/" +
                     modelName + ":generateContent?key=" + apiKey;

        String escapedPrompt = objectMapper.writeValueAsString(prompt);

        String requestBody = """
            {
                "contents": [
                    {
                        "parts": [
                            {
                                "text": %s
                            }
                        ]
                    }
                ],
                "generationConfig": {
                    "temperature": 0.3,
                    "maxOutputTokens": 2048
                }
            }
            """.formatted(escapedPrompt);
        // ↑ Increased from 800 → 2048 to give the model enough room for a full summary.

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(requestBody))
                .timeout(Duration.ofSeconds(60))
                .build();

        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

        if (response.statusCode() != 200) {
            throw new RuntimeException("HTTP " + response.statusCode() + " from " + modelName + ": " + response.body());
        }

        JsonNode root = objectMapper.readTree(response.body());

        JsonNode candidate = root.path("candidates").get(0);
        if (candidate == null) {
            throw new RuntimeException("No candidates in response from " + modelName);
        }

        // FIX: Check finishReason BEFORE returning the text.
        // If MAX_TOKENS → the summary is cut off → throw TruncatedResponseException
        // so the failover loop moves to the next model.
        String finishReason = candidate.path("finishReason").asText("");
        if ("MAX_TOKENS".equalsIgnoreCase(finishReason)) {
            throw new TruncatedResponseException(
                "Model " + modelName + " hit token limit (MAX_TOKENS). Response is incomplete."
            );
        }

        // Also guard against SAFETY or other non-STOP reasons
        if (!"STOP".equalsIgnoreCase(finishReason) && !finishReason.isEmpty()) {
            throw new RuntimeException(
                "Model " + modelName + " stopped with unexpected reason: " + finishReason
            );
        }

        JsonNode textNode = candidate
                .path("content")
                .path("parts")
                .get(0)
                .path("text");

        if (textNode == null || textNode.isMissingNode() || textNode.asText().isBlank()) {
            throw new RuntimeException("Model " + modelName + " returned an empty response body.");
        }

        return textNode.asText();
    }

    /**
     * Thrown specifically when a model returns a response cut off by the token limit.
     * Allows the failover loop to distinguish truncation from a hard error.
     */
    private static class TruncatedResponseException extends Exception {
        public TruncatedResponseException(String message) {
            super(message);
        }
    }
}