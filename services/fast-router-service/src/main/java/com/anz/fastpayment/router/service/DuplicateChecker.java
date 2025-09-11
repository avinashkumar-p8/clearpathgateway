package com.anz.fastpayment.router.service;

import com.anz.fastpayment.router.model.DedupKey;
import com.google.cloud.spring.data.spanner.core.SpannerTemplate;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.annotation.Autowired;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.stereotype.Component;

@Component
public class DuplicateChecker {

    private static final Logger log = LoggerFactory.getLogger(DuplicateChecker.class);

    private final SpannerTemplate spannerTemplate;
    private final java.util.concurrent.ConcurrentHashMap<String, Long> recentKeys = new java.util.concurrent.ConcurrentHashMap<>();
    private final long cacheTtlMillis;

    @Autowired
    public DuplicateChecker(SpannerTemplate spannerTemplate,
                            @Value("${app.router.duplicate-cache-ttl-minutes:60}") long cacheTtlMinutes) {
        this.spannerTemplate = spannerTemplate;
        this.cacheTtlMillis = java.util.concurrent.TimeUnit.MINUTES.toMillis(cacheTtlMinutes);
    }

    // Backward-compatible constructor for existing tests
    public DuplicateChecker(SpannerTemplate spannerTemplate) {
        this(spannerTemplate, 60L);
    }

    // Returns true if duplicate (i.e., key already exists). First-time insert returns false (not duplicate).
    public boolean isDuplicateAndRecord(String messageType, String uniqueId, String xml) {
        if (uniqueId == null || uniqueId.isBlank()) {
            log.info("[DEDUP] No uniqueId provided for messageType={}; skipping dedup.", messageType);
            return false;
        }
        final String cacheKey = messageType + "::" + uniqueId.trim();
        if (isPresentAndFresh(cacheKey)) {
            log.info("[DEDUP] Duplicate detected via in-memory cache for messageType={}, uniqueId={}", messageType, uniqueId);
            return true;
        }
        try {
            DedupKey key = new DedupKey();
            key.setMessageType(messageType);
            key.setUniqueId(uniqueId.trim());
            spannerTemplate.insert(key); // INSERT only; fails if key exists
            log.debug("[DEDUP] Inserted dedup key for messageType={}, uniqueId={}", messageType, uniqueId);
            // Record in cache to prevent immediate duplicates even if store hiccups later
            recentKeys.put(cacheKey, System.currentTimeMillis());
            return false; // not a duplicate
        } catch (DuplicateKeyException dke) {
            log.info("[DEDUP] Duplicate detected for messageType={}, uniqueId={}", messageType, uniqueId);
            recentKeys.put(cacheKey, System.currentTimeMillis());
            return true;
        } catch (RuntimeException re) {
            String msg = re.getMessage() == null ? "" : re.getMessage();
            if (msg.contains("ALREADY_EXISTS") || msg.contains("AlreadyExists") || msg.contains("already exists")) {
                log.info("[DEDUP] Duplicate detected for messageType={}, uniqueId={}", messageType, uniqueId);
                recentKeys.put(cacheKey, System.currentTimeMillis());
                return true;
            }
            // Best-effort: do not block processing on dedup store failure
            log.warn("[DEDUP] Dedup insert failed (non-fatal) for messageType={}, uniqueId={}, err={}", messageType, uniqueId, re.toString());
            // Still seed cache so an immediate repeat is treated as duplicate within TTL window
            recentKeys.put(cacheKey, System.currentTimeMillis());
            return false;
        }
    }

    private boolean isPresentAndFresh(String cacheKey) {
        try {
            Long ts = recentKeys.get(cacheKey);
            long now = System.currentTimeMillis();
            if (ts == null) return false;
            if (now - ts <= cacheTtlMillis) return true;
            // Expired - cleanup
            recentKeys.remove(cacheKey, ts);
            return false;
        } catch (Exception ignore) {
            return false;
        }
    }
}


