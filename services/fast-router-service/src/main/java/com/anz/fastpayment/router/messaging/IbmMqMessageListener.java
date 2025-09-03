package com.anz.fastpayment.router.messaging;

import com.anz.fastpayment.router.service.RouterOrchestrator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.jms.annotation.JmsListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

@Component
@Profile({"gcp","prod"})
public class IbmMqMessageListener {

    private static final Logger log = LoggerFactory.getLogger(IbmMqMessageListener.class);

    private final RouterOrchestrator routerOrchestrator;

    @Value("${app.logging.payload-preview-enabled:false}")
    private boolean payloadPreviewEnabled;

    public IbmMqMessageListener(RouterOrchestrator routerOrchestrator) {
        this.routerOrchestrator = routerOrchestrator;
    }

    @JmsListener(destination = "${app.mq.input-queue:payment.inbound}")
    public void onMessage(@Payload String payload) {
        String messageKey = UUID.randomUUID().toString();
        int sizeBytes = payload == null ? 0 : payload.getBytes(StandardCharsets.UTF_8).length;
        log.info("[IBM-MQ] Received message key={}, size={} bytes", messageKey, sizeBytes);
        if (payload == null || payload.isBlank()) {
            log.warn("[IBM-MQ] Ignoring empty/null payload for key={}", messageKey);
            return;
        }
        if (payloadPreviewEnabled && log.isDebugEnabled()) {
            log.debug("[IBM-MQ] Payload preview (first 500 chars) key={} => {}", messageKey,
                    safePreview(payload, 500));
        }
        try {
            routerOrchestrator.processInboundXml(payload);
        } catch (Exception e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            String preview = safePreview(payload, 200);
            log.error("[IBM-MQ] Error processing message key={}, sizeBytes={}, preview='{}'", messageKey, sizeBytes, preview, e);
            throw e;
        }
    }

    private String safePreview(String s, int maxChars) {
        if (s == null) return "<empty>";
        if (s.isBlank()) return "<blank>";
        String cut = s.substring(0, Math.min(maxChars, s.length()));
        cut = cut.replaceAll("\\d{8,}", "***masked***");
        cut = cut.replaceAll("(?i)(<IBAN>)(.*?)(</IBAN>)", "$1***masked***$3");
        cut = cut.replaceAll("(?i)(<(?:AcctNbr|AccountNumber|CardNbr|CardNumber|PAN)>)(.*?)(</\\s*(?:AcctNbr|AccountNumber|CardNbr|CardNumber|PAN)>)", "$1***masked***$3");
        return cut;
    }
}





