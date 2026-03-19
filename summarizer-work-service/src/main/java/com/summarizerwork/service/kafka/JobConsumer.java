package com.summarizerwork.service.kafka;

import java.time.LocalDateTime;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.summarizerwork.service.dto.CachedJob;
import com.summarizerwork.service.model.Job;
import com.summarizerwork.service.repository.JobRepository;
import com.summarizerwork.service.service.CacheService;
import com.summarizerwork.service.service.ContentFetcherService;
import com.summarizerwork.service.service.GeminiService;
import com.summarizerwork.service.util.APICONST;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
@RequiredArgsConstructor
public class JobConsumer implements APICONST {

	private final JobRepository jobRepository;
	private final CacheService cacheService;
	private final ContentFetcherService contentFetcherService;
	private final GeminiService geminiService;
	private final ObjectMapper objectMapper = new ObjectMapper();

	@KafkaListener(topics = "${kafka.topic}", groupId = "${kafka.group-id}")
	public void consume(String messageJson) {

		String jobId = "unknown";
		long startTime = System.currentTimeMillis();

		try {
			// Parse Kafka message
			JsonNode message = objectMapper.readTree(messageJson);
			jobId = message.get("job_id").asText();
			String contentHash = message.get("content_hash").asText();
			String type = message.get("type").asText();
			String url = message.has("url") ? message.get("url").asText() : null;
			String text = message.has("text") ? message.get("text").asText() : null;

			final String finalJobId = jobId;

			log.info("WORKER]Picked up job ", finalJobId);

			// Load job from DB
			Job job = jobRepository.findById(finalJobId)
					.orElseThrow(() -> new RuntimeException("Job not found in DB: " + finalJobId));

			// Mark PROCESSING → save DB + Redis
			job.setStatus(PROCESSING);
			job.setUpdatedAt(LocalDateTime.now());
			jobRepository.save(job);
			cacheService.cacheJob(job); // Redis now shows PROCESSING
			log.info("WORKER Job marked as PROCESSING", finalJobId);

			// Check Redis cache before calling Gemini
			CachedJob cachedResult = cacheService.getCachedSummary(contentHash);

			String summary;
			boolean fromCache;

			if (cachedResult != null && cachedResult.getSummary() != null) {
				// Cache HIT — reuse existing summary, skip Gemini
				log.info("WORKER Cache HIT for job  — skipping Gemini", finalJobId);
				summary = cachedResult.getSummary();
				fromCache = true;

			} else {
				// Cache MISS — fetch content and call Gemini
				log.info("WORKER Cache MISS for job — calling Gemini", finalJobId);

				String content = contentFetcherService.fetchContent(type, url, text);
				summary = geminiService.summarize(content);
				fromCache = false;
			}

			long processingTime = System.currentTimeMillis() - startTime;

			// Mark COMPLETED → save DB + Redis
			job.setStatus(COMPLETED);
			job.setSummary(summary);
			job.setCached(fromCache);
			job.setProcessingTimeMs(processingTime);
			job.setUpdatedAt(LocalDateTime.now());
			jobRepository.save(job); // DB updated
			cacheService.cacheJob(job); // Redis updated with COMPLETED + summary

			log.info("WORKER Job COMPLETED in ms (fromCache=)", finalJobId, processingTime, fromCache);

		} catch (Exception e) {
			log.error("WORKER Job FAILED:", jobId, e.getMessage());
			markJobFailed(jobId, e.getMessage());
		}
	}

	// PRIVATE — mark job failed in DB + Redis

	private void markJobFailed(String jobId, String errorMessage) {
		try {
			jobRepository.findById(jobId).ifPresent(job -> {
				job.setStatus(FAILED);
				job.setErrorMessage(errorMessage);
				job.setUpdatedAt(LocalDateTime.now());
				jobRepository.save(job); // DB updated
				cacheService.cacheJob(job); // Redis updated with FAILED status
				log.info("WORKER Job saved as FAILED", jobId);
			});
		} catch (Exception e) {
			log.error("WORKER Could not save FAILED status for job: ", jobId, e.getMessage());
		}
	}
}