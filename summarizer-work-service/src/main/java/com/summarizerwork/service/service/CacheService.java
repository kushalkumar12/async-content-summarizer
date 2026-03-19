package com.summarizerwork.service.service;

import java.util.concurrent.TimeUnit;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.summarizerwork.service.dto.CachedJob;
import com.summarizerwork.service.model.Job;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
@RequiredArgsConstructor
public class CacheService {

    private final StringRedisTemplate redisTemplate;
    private final ObjectMapper objectMapper = new ObjectMapper();

    private static final String HASH_KEY_PREFIX = "summary:cache:"; // lookup by contentHash
    private static final String JOB_KEY_PREFIX  = "job:id:";        // lookup by jobId
    private static final long   TTL_HOURS       = 24;

    // WRITE — store under both keys

    public void cacheJob(Job job) {
        if (job == null || job.getContentHash() == null) {
            log.warn("CACHE Skipping null or incomplete job");
            return;
        }

        try {
            CachedJob cachedJob = mapToCachedJob(job);
            String json = objectMapper.writeValueAsString(cachedJob);

            // Key 1: by contentHash → used by API submitJob for dedup
            redisTemplate.opsForValue().set(
                HASH_KEY_PREFIX + job.getContentHash(),
                json, TTL_HOURS, TimeUnit.HOURS
            );

            // Key 2: by jobId → used by API getStatus and getResult
            redisTemplate.opsForValue().set(
                JOB_KEY_PREFIX + job.getId(),
                json, TTL_HOURS, TimeUnit.HOURS
            );

            log.info(" CACHEStored job under both keys (status=)", job.getId(), job.getStatus());

        } catch (Exception e) {
            // Never crash main flow on cache write failure
            log.error("[CACHE] Failed to store job  : ", job.getId(), e.getMessage());
        }
    }

    // READ — by contentHash (used by Worker before calling Gemini)

    public CachedJob getCachedSummary(String contentHash) {
        return readFromRedis(HASH_KEY_PREFIX + contentHash, "contentHash", contentHash);
    }

    // READ — by jobId (used by API getStatus, getResult)

    public CachedJob getCachedSummaryByJobId(String jobId) {
        return readFromRedis(JOB_KEY_PREFIX + jobId, "jobId", jobId);
    }

    // PRIVATE HELPERS

    private CachedJob readFromRedis(String key, String lookupType, String lookupValue) {
        try {
            String json = redisTemplate.opsForValue().get(key);

            if (json == null) {
                log.info("CACHE MISS — {}={}", lookupType, lookupValue);
                return null;
            }

            CachedJob cachedJob = objectMapper.readValue(json, CachedJob.class);
            log.info("CACHE HIT — {}={} (status={})", lookupType, lookupValue, cachedJob.getStatus());
            return cachedJob;

        } catch (Exception e) {
            log.error("CACHE Read error for {}={}: {}", lookupType, lookupValue, e.getMessage());
            return null; // treat as miss, never crash
        }
    }

    private CachedJob mapToCachedJob(Job job) {
        CachedJob dto = new CachedJob();
        dto.setJobId(job.getId());
        dto.setStatus(job.getStatus());
        dto.setContentHash(job.getContentHash());
        dto.setOriginalUrl(job.getOriginalUrl());
        dto.setInputText(job.getInputText());
        dto.setSummary(job.getSummary());
        dto.setErrorMessage(job.getErrorMessage());
        dto.setCached(job.getCached());
        dto.setProcessingTimeMs(job.getProcessingTimeMs());
        dto.setCreatedAt(job.getCreatedAt() != null ? job.getCreatedAt().toString() : null);
        dto.setUpdatedAt(job.getUpdatedAt() != null ? job.getUpdatedAt().toString() : null);
        return dto;
    }
}