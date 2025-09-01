package com.anz.fastpayment.router.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class DuplicateChecker {

    private static final Logger log = LoggerFactory.getLogger(DuplicateChecker.class);

    // Placeholder: log-only; always allow processing to continue
    public boolean isDuplicateAndRecord(String messageType, String uniqueId, String xml) {
        String basis = (uniqueId != null && !uniqueId.isBlank()) ? (messageType + "::" + uniqueId) : "<hash-placeholder>";
        log.info("[DEDUP] Placeholder check for basis={}, allowing message", basis);
        return false;
    }
}


