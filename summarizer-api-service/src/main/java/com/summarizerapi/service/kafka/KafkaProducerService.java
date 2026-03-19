package com.summarizerapi.service.kafka;

import java.util.HashMap;
import java.util.Map;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
@RequiredArgsConstructor
public class KafkaProducerService {
	private final KafkaTemplate<String, String> kafkaTemplate;
	private final ObjectMapper objectMapper = new ObjectMapper();

	@Value("${kafka.topic}")
	private String topic;

	public void sendJob(String jobId, String contentHash, String type, String url, String text) {
		try {
			// Build a simple JSON message
			Map<String, String> message = new HashMap<>();
			message.put("job_id", jobId);
			message.put("content_hash", contentHash);
			message.put("type", type); // "URL" or "TEXT"
			if (url != null)
				message.put("url", url);
			if (text != null)
				message.put("text", text);

			String json = objectMapper.writeValueAsString(message);

			kafkaTemplate.send(topic, jobId, json);
			log.info("Sent job to Kafka topic", jobId, topic);

		} catch (Exception e) {
			log.error("Failed to send job to Kafka: ", jobId, e.getMessage());
			throw new RuntimeException("Failed to queue job", e);
		}
	}
}
