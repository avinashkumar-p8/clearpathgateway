package com.anz.fastpayment.router.service;

import com.anz.fastpayment.router.model.DedupKey;
import com.google.cloud.spring.data.spanner.core.SpannerTemplate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.stereotype.Component;

@Component
public class DuplicateChecker {

    private static final Logger log = LoggerFactory.getLogger(DuplicateChecker.class);

    private final SpannerTemplate spannerTemplate;

    public DuplicateChecker(SpannerTemplate spannerTemplate) {
        this.spannerTemplate = spannerTemplate;
    }

    // Returns true if duplicate (i.e., key already exists). First-time insert returns false (not duplicate).
    public boolean isDuplicateAndRecord(String messageType, String uniqueId, String xml) {
        if (uniqueId == null || uniqueId.isBlank()) {
            log.info("[DEDUP] No uniqueId provided for messageType={}; skipping dedup.", messageType);
            return false;
        }
        try {
            DedupKey key = new DedupKey();
            key.setMessageType(messageType);
            key.setUniqueId(uniqueId.trim());
            spannerTemplate.insert(key); // INSERT only; fails if key exists
            log.debug("[DEDUP] Inserted dedup key for messageType={}, uniqueId={}", messageType, uniqueId);
            return false; // not a duplicate
        } catch (DuplicateKeyException dke) {
            log.info("[DEDUP] Duplicate detected for messageType={}, uniqueId={}", messageType, uniqueId);
            return true;
        } catch (RuntimeException re) {
            String msg = re.getMessage() == null ? "" : re.getMessage();
            if (msg.contains("ALREADY_EXISTS") || msg.contains("AlreadyExists") || msg.contains("already exists")) {
                log.info("[DEDUP] Duplicate detected for messageType={}, uniqueId={}", messageType, uniqueId);
                return true;
            }
            // Best-effort: do not block processing on dedup store failure
            log.warn("[DEDUP] Dedup insert failed (non-fatal) for messageType={}, uniqueId={}, err={}", messageType, uniqueId, re.toString());
            return false;
        }
    }
}


