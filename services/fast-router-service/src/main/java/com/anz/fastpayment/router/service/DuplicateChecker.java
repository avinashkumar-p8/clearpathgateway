package com.anz.fastpayment.router.service;

import com.anz.fastpayment.router.model.DedupeKey;
import com.anz.fastpayment.router.repository.DedupeKeyRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.stereotype.Component;

@Component
public class DuplicateChecker {

    private static final Logger log = LoggerFactory.getLogger(DuplicateChecker.class);

    private final DedupeKeyRepository dedupeRepo;

    public DuplicateChecker(ObjectProvider<DedupeKeyRepository> repoProvider) {
        this.dedupeRepo = repoProvider.getIfAvailable();
    }

    public boolean isDuplicateAndRecord(String messageType, String uniqueId, String xml) {
        String basis = (uniqueId != null && !uniqueId.isBlank()) ? (messageType + "::" + uniqueId) : null;
        if (basis == null) {
            log.info("[DEDUP] No uniqueId; skipping dedupe");
            return false;
        }
        try {
            if (dedupeRepo == null) {
                log.info("[DEDUP] Repo unavailable (local?); allowing basis={}", basis);
                return false;
            }
            boolean exists = dedupeRepo.existsById(basis);
            if (exists) {
                log.info("[DEDUP] Duplicate detected for basis={}, blocking", basis);
                return true;
            }
            DedupeKey key = new DedupeKey(basis, java.time.Instant.now());
            dedupeRepo.save(key);
            log.info("[DEDUP] Recorded new basis={}, allowing", basis);
            return false;
        } catch (Exception e) {
            log.warn("[DEDUP] Error during dedupe check; allowing. err={}", e.getMessage());
            return false;
        }
    }
}


