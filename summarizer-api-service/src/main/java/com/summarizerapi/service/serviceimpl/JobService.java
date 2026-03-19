package com.summarizerapi.service.serviceimpl;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.LocalDateTime;
import java.util.Optional;
import java.util.UUID;

import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;

import com.summarizerapi.service.dto.CachedJob;
import com.summarizerapi.service.dto.ResultResponse;
import com.summarizerapi.service.dto.StatusResponse;
import com.summarizerapi.service.dto.SubmitResponse;
import com.summarizerapi.service.kafka.KafkaProducerService;
import com.summarizerapi.service.model.Job;
import com.summarizerapi.service.repository.JobRepository;
import com.summarizerapi.service.util.APICONST;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
@RequiredArgsConstructor
public class JobService implements APICONST {

    private final JobRepository jobRepository;
    private final KafkaProducerService kafkaProducerService;
    private final CacheService cacheService;

    // *1*****************************************************************************
    // SUBMIT API 
    // ******************************************************************************

    public SubmitResponse submitJob(String url, String text) {

        // 1. Validate input fields URL or Text--------------------------------------------------------------------------
        validateInput(url, text);

        String type    = (url != null) ? URL : TEXT;
        String content = (url != null) ? url.trim() : text.trim();
        String contentHash = sha256(content);

        // 2. Check Redis cache first--------------------------------------------------------------------------
        CachedJob cachedJob = cacheService.getCachedSummary(contentHash);
        if (cachedJob != null) {
            log.info("[SUBMIT] Redis Cache HIT — returning job { }  from Redis", cachedJob.getJobId());
            return new SubmitResponse(cachedJob.getJobId(), cachedJob.getStatus());
        }

        // 3. Check DB for existing job with same content--------------------------------------------------------------------------
        Optional<Job> existingJob = jobRepository.findTopByContentHashOrderByCreatedAtDesc(contentHash);
        if (existingJob.isPresent()) {
            Job existing = existingJob.get();

            // FAILED jobs must be retried — fall through to new job creation
            if (FAILED.equals(existing.getStatus())) {
                log.info("[SUBMIT] Previous job {} failed — creating new job", existing.getId());
            } else {
                log.info("[SUBMIT] DB HIT — job {} already exists with status {}", existing.getId(), existing.getStatus());
                return buildSubmitResponse(existing);
            }
        }

        // 4. Create new job--------------------------------------------------------------------------
        Job job = buildNewJob(contentHash, url, text);

        try {
            jobRepository.save(job);
        } catch (DataIntegrityViolationException e) {
            // Race condition: another thread inserted same hash at the same time
            Job raceJob = jobRepository
                    .findTopByContentHashOrderByCreatedAtDesc(contentHash)
                    .orElseThrow(() -> new RuntimeException("Race condition: job not found after conflict"));
            log.warn("[SUBMIT] Race condition handled — returning existing job {}", raceJob.getId());
            return buildSubmitResponse(raceJob);
        }

        // 5. Push to Kafka for async processing--------------------------------------------------------------------------
        kafkaProducerService.sendJob(job.getId(), contentHash, type, url, text);
        log.info("[SUBMIT] New job {} queued successfully", job.getId());

        return new SubmitResponse(job.getId(), QUEUED);
    }

    // **2****************************************************************************
    // STATUS
    // ******************************************************************************

    public StatusResponse getStatus(String jobId) {

        // 1. Check Redis first--------------------------------------------------------------------------
        CachedJob cachedJob = cacheService.getCachedSummaryByJobId(jobId);
        if (cachedJob != null) {
            log.info("[STATUS] Cache HIT — job {} from Redis", jobId);
            return new StatusResponse(
                cachedJob.getJobId(),
                cachedJob.getStatus(),
                cachedJob.getCreatedAt()
            );
        }

        // 2. Fallback to DB--------------------------------------------------------------------------
        log.info("[STATUS] Cache MISS — fetching job {} from DB", jobId);
        Job job = findJobOrThrow(jobId);

        return new StatusResponse(
            job.getId(),
            job.getStatus(),
            job.getCreatedAt().toString()
        );
    }

    // ***3***************************************************************************
    // RESULT
    //  ******************************************************************************

    public ResultResponse getResult(String jobId) {

        // 1. Check Redis first--------------------------------------------------------------------------
        CachedJob cachedJob = cacheService.getCachedSummaryByJobId(jobId);
        if (cachedJob != null) {
            log.info("[RESULT] Cache HIT — job {} from Redis", jobId);
            ensureCompleted(cachedJob.getStatus(), jobId);
            return new ResultResponse(
                cachedJob.getJobId(),
                cachedJob.getOriginalUrl(),
                cachedJob.getSummary(),
                cachedJob.getCached(),
                cachedJob.getProcessingTimeMs()
            );
        }

        // 2. Fallback to DB--------------------------------------------------------------------------
        log.info("[RESULT] Cache MISS — fetching job {} from DB", jobId);
        Job job = findJobOrThrow(jobId);
        ensureCompleted(job.getStatus(), jobId);

        return new ResultResponse(
            job.getId(),
            job.getOriginalUrl(),
            job.getSummary(),
            job.getCached(),
            job.getProcessingTimeMs()
        );
    }

    // ****4********************************************************************************************
    // PRIVATE HELPERS
    // *************************************************************************************************

    private void validateInput(String url, String text) {
        if (url == null && text == null) {
            throw new IllegalArgumentException("Either 'url' or 'text' must be provided");
        }
        if (url != null && text != null) {
            throw new IllegalArgumentException("Provide only 'url' OR 'text', not both");
        }
        String content = (url != null) ? url : text;
        if (content.trim().isEmpty()) {
            throw new IllegalArgumentException("Content cannot be empty");
        }
    }

    private Job buildNewJob(String contentHash, String url, String text) {
        Job job = new Job();
        job.setId(UUID.randomUUID().toString());
        job.setStatus(QUEUED);
        job.setContentHash(contentHash);
        job.setOriginalUrl(url);
        job.setInputText(text);
        job.setCached(false);
        job.setCreatedAt(LocalDateTime.now());
        job.setUpdatedAt(LocalDateTime.now());
        return job;
    }

    private SubmitResponse buildSubmitResponse(Job job) {
        return new SubmitResponse(job.getId(), job.getStatus());
    }

    private Job findJobOrThrow(String jobId) {
        return jobRepository.findById(jobId)
            .orElseThrow(() -> new RuntimeException("Job not found: " + jobId));
    }

    private void ensureCompleted(String status, String jobId) {
        if (!COMPLETED.equals(status)) {
            throw new RuntimeException(
                "Job " + jobId + " is not completed yet. Current status: " + status
            );
        }
    }

    private String sha256(String input) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(input.getBytes(StandardCharsets.UTF_8));
            StringBuilder hex = new StringBuilder();
            for (byte b : hash) {
                hex.append(String.format("%02x", b));
            }
            return hex.toString();
        } catch (Exception e) {
            throw new RuntimeException("Failed to hash content", e);
        }
    }
}