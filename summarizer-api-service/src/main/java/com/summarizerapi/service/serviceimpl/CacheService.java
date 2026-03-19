package com.summarizerapi.service.serviceimpl;

import java.util.concurrent.TimeUnit;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.summarizerapi.service.dto.CachedJob;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
@RequiredArgsConstructor
public class CacheService {

    private final StringRedisTemplate redisTemplate;
    private final ObjectMapper objectMapper = new ObjectMapper();

    private static final String HASH_KEY_PREFIX = "summary:cache:";   // lookup by content hash
    private static final String JOB_KEY_PREFIX  = "job:id:";          // lookup by jobId
    private static final long   TTL_HOURS       = 24;

    // READ — by content hash (used at submit time)

    public CachedJob getCachedSummary(String contentHash) {
        return getFromRedis(HASH_KEY_PREFIX + contentHash, "contentHash", contentHash);
    }

    // READ — by jobId (used by getStatus, getResult)

    public CachedJob getCachedSummaryByJobId(String jobId) {
        return getFromRedis(JOB_KEY_PREFIX + jobId, "jobId", jobId);
    }

    // WRITE — store under both keys

    public void cacheJob(CachedJob cachedJob) {
        if (cachedJob == null) {
            log.warn("Attempted to cache a null CachedJob — skipping");
            return;
        }

        try {
            String json = objectMapper.writeValueAsString(cachedJob);

            // Key 1: by content hash → used for dedup on submit
            redisTemplate.opsForValue().set(
                HASH_KEY_PREFIX + cachedJob.getContentHash(),
                json, TTL_HOURS, TimeUnit.HOURS
            );

            // Key 2: by jobId → used by getStatus and getResult
            redisTemplate.opsForValue().set(
                JOB_KEY_PREFIX + cachedJob.getJobId(),
                json, TTL_HOURS, TimeUnit.HOURS
            );

            log.info("[CACHE] Stored job {} under both keys (TTL: {}h)",
                cachedJob.getJobId(), TTL_HOURS);

        } catch (Exception e) {
            // Cache write failure must never crash the main flow
            log.error("[CACHE] Failed to store job {}: {}", cachedJob.getJobId(), e.getMessage());
        }
    }

    // DELETE — evict both keys (e.g. on job retry)

    public void evictJob(String jobId, String contentHash) {
        try {
            redisTemplate.delete(HASH_KEY_PREFIX + contentHash);
            redisTemplate.delete(JOB_KEY_PREFIX + jobId);
            log.info("[CACHE] Evicted keys for job {}", jobId);
        } catch (Exception e) {
            log.error("[CACHE] Failed to evict job {}: {}", jobId, e.getMessage());
        }
    }

    // PRIVATE — shared read logic

    private CachedJob getFromRedis(String key, String lookupType, String lookupValue) {
        try {
            String json = redisTemplate.opsForValue().get(key);

            if (json == null) {
                log.info("[CACHE] MISS — {}={}", lookupType, lookupValue);
                return null;
            }

            CachedJob cachedJob = objectMapper.readValue(json, CachedJob.class);
            log.info("[CACHE] HIT — {}={}", lookupType, lookupValue);
            return cachedJob;

        } catch (Exception e) {
            // Corrupted or old data in Redis — treat as miss, never crash
            log.error("[CACHE] Read error for {}={} : {}", lookupType, lookupValue, e.getMessage());
            return null;
        }
    }
}