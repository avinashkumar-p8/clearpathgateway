package com.anz.fastpayment.router.messaging;

import com.anz.fastpayment.router.service.RouterOrchestrator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

abstract class AbstractJmsMessageListener {

    private static final Logger log = LoggerFactory.getLogger(AbstractJmsMessageListener.class);

    protected final RouterOrchestrator routerOrchestrator;

    @Value("${app.logging.payload-preview-enabled:false}")
    private boolean payloadPreviewEnabled;

    protected AbstractJmsMessageListener(RouterOrchestrator routerOrchestrator) {
        this.routerOrchestrator = routerOrchestrator;
    }

    protected void handleMessage(String payload, String logPrefix) {
        String messageKey = UUID.randomUUID().toString();
        int sizeBytes = payload == null ? 0 : payload.getBytes(StandardCharsets.UTF_8).length;
        log.info("{} Received message key={}, size={} bytes", logPrefix, messageKey, sizeBytes);
        if (payload == null || payload.isBlank()) {
            log.warn("{} Ignoring empty/null payload for key={}", logPrefix, messageKey);
            return;
        }
        if (payloadPreviewEnabled && log.isDebugEnabled()) {
            log.debug("{} Payload preview (first 500 chars) key={} => {}", logPrefix, messageKey,
                    safePreview(payload, 500));
        }
        try {
            routerOrchestrator.processInboundXml(payload);
        } catch (Exception e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            String preview = safePreview(payload, 200);
            log.error("{} Error processing message key={}, sizeBytes={}, preview='{}'", logPrefix, messageKey, sizeBytes, preview, e);
            throw e;
        }
    }

    protected String safePreview(String s, int maxChars) {
        if (s == null) return "<empty>";
        if (s.isBlank()) return "<blank>";
        String cut = s.substring(0, Math.min(maxChars, s.length()));
        cut = cut.replaceAll("\\d{8,}", "***masked***");
        cut = cut.replaceAll("(?i)(<IBAN>)(.*?)(</IBAN>)", "$1***masked***$3");
        cut = cut.replaceAll("(?i)(<(?:AcctNbr|AccountNumber|CardNbr|CardNumber|PAN)>)(.*?)(</\\s*(?:AcctNbr|AccountNumber|CardNbr|CardNumber|PAN)>)", "$1***masked***$3");
        return cut;
    }
}


