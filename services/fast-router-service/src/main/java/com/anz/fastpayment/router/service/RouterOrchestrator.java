package com.anz.fastpayment.router.service;

import com.anz.fastpayment.router.model.InboundMessage;
import com.anz.fastpayment.router.repository.InboundMessageRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import com.anz.fastpayment.router.model.UnifiedMessage;
import com.anz.fastpayment.router.repository.UnifiedMessageRepository;
import org.springframework.beans.factory.ObjectProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.Instant;

@Service
public class RouterOrchestrator {

    private static final Logger log = LoggerFactory.getLogger(RouterOrchestrator.class);

    private final PuidGenerator puidGenerator;
    private final InboundMessageRepository inboundRepo; // optional in local profile
    private final EventPublisher eventPublisher;
    private final Iso20022MessageTypeDetector typeDetector;
    private final XmlSchemaValidator xmlSchemaValidator;
    private final Iso20022Transformer transformer;
    private final KafkaPublisher kafkaPublisher;
    private final UniqueIdExtractor uniqueIdExtractor;
    private final UnifiedMessageRepository unifiedRepo;
    private final DuplicateChecker duplicateChecker;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final MeterRegistry meterRegistry;
    private final Timer xsdTimer;
    private final Timer transformTimer;
    private final Timer publishTimer;
    private final Counter xsdFailCounter;
    private final Counter transformFailCounter;

    public RouterOrchestrator(PuidGenerator puidGenerator,
                              ObjectProvider<InboundMessageRepository> inboundRepoProvider,
                              Iso20022MessageTypeDetector typeDetector,
                              XmlSchemaValidator xmlSchemaValidator,
                              Iso20022Transformer transformer,
                              KafkaPublisher kafkaPublisher,
                              EventPublisher eventPublisher,
                              UniqueIdExtractor uniqueIdExtractor,
                              ObjectProvider<UnifiedMessageRepository> unifiedRepoProvider,
                              DuplicateChecker duplicateChecker,
                              MeterRegistry meterRegistry) {
        this.puidGenerator = puidGenerator;
        this.inboundRepo = inboundRepoProvider.getIfAvailable();
        this.typeDetector = typeDetector;
        this.xmlSchemaValidator = xmlSchemaValidator;
        this.transformer = transformer;
        this.kafkaPublisher = kafkaPublisher;
        this.eventPublisher = eventPublisher;
        this.uniqueIdExtractor = uniqueIdExtractor;
        this.unifiedRepo = unifiedRepoProvider.getIfAvailable();
        this.duplicateChecker = duplicateChecker;
        this.meterRegistry = meterRegistry;
        this.xsdTimer = meterRegistry.timer("router.xsd.validate.ms");
        this.transformTimer = meterRegistry.timer("router.transform.ms");
        this.publishTimer = meterRegistry.timer("router.publish.ms");
        this.xsdFailCounter = meterRegistry.counter("router.xsd.fail.count");
        this.transformFailCounter = meterRegistry.counter("router.transform.fail.count");
    }

    public void processInboundXml(String xml) {
        log.info("[PROC] Starting processing for inbound message");
        String puid = puidGenerator.nextPuid();
        log.info("[PUID] Generated PUID={}", puid);
        if (xml == null || xml.trim().isEmpty()) {
            log.error("[PROC] Blank XML received; aborting. PUID={}", puid);
            return;
        }
        String messageType = typeDetector.detectType(xml);
        log.info("[DETECT] Detected messageType={}", messageType);
        if (messageType == null || messageType.trim().isEmpty()) {
            log.error("[PROC] Could not detect messageType; aborting. PUID={}", puid);
            return;
        }

        // Best-effort persist RECEIVED
        try {
            if (inboundRepo != null) {
                InboundMessage m = new InboundMessage();
                m.setPuid(puid);
                m.setChannelId("G3I");
                m.setMessageType(messageType);
                m.setReceivedAt(java.time.Instant.now());
                m.setRawXml(xml);
                m.setStatus("RECEIVED");
                inboundRepo.save(m);
            }
        } catch (Exception e) {
            log.warn("[PERSIST] RECEIVED save failed PUID={}, err={}", puid, e.getMessage());
        }

        // XSD validation - only this error triggers pacs.002
        try {
            log.info("[XSD] Validating XML against XSD for type={}", messageType);
            xsdTimer.record(() -> xmlSchemaValidator.validate(xml, messageType));
            log.info("[XSD] Validation successful for PUID={}", puid);
            try {
                if (inboundRepo != null) {
                    InboundMessage m = inboundRepo.findById(puid).orElse(null);
                    if (m != null) { m.setStatus("VALIDATED"); inboundRepo.save(m); }
                }
            } catch (Exception ignore) { }
        } catch (Exception xsdEx) {
            log.error("[XSD] Validation failed PUID={}, type={}, err={}", puid, messageType, xsdEx.getMessage());
            xsdFailCounter.increment();
            kafkaPublisher.publishInvalid(puid, xml);
            try {
                String uniqueId = uniqueIdExtractor.extractUniqueId(xml, messageType);
                com.fasterxml.jackson.databind.node.ObjectNode node = objectMapper.createObjectNode();
                node.put("puid", puid);
                node.put("messageType", messageType);
                if (uniqueId == null || uniqueId.isBlank()) node.putNull("uniqueId"); else node.put("uniqueId", uniqueId);
                node.put("error", xsdEx.getMessage());
                node.put("originalXml", xml);
                String payload = objectMapper.writeValueAsString(node);
                kafkaPublisher.publishPacs002Request(puid, payload);
            } catch (Exception e) {
                log.warn("[KAFKA] Failed to publish pacs002 request PUID={}, err={}", puid, e.getMessage());
            }
            try { eventPublisher.publishPaymentReceivedEvent(puid, "G3I", "exception-queue"); } catch (Exception ignore) { }
            try {
                if (inboundRepo != null) {
                    InboundMessage m = inboundRepo.findById(puid).orElse(null);
                    if (m != null) { m.setStatus("ERROR"); m.setError("XSD_FAIL"); inboundRepo.save(m); }
                }
            } catch (Exception ignore) { }
            return;
        }

        // Dedup placeholder (currently non-blocking)
        try {
            String uniqueId = uniqueIdExtractor.extractUniqueId(xml, messageType);
            if (uniqueId == null || uniqueId.trim().isEmpty()) {
                log.info("[DEDUP] Skipping dedup for PUID={} due to missing uniqueId", puid);
            } else if (duplicateChecker.isDuplicateAndRecord(messageType, uniqueId, xml)) {
                log.info("[DEDUP] Skipping duplicate message for PUID={} (uniqueId={})", puid, uniqueId);
                return;
            }
        } catch (Exception ignore) { }

        // Transform to unified JSON
        String unifiedJson;
        try {
            log.info("[TRANSFORM] Transforming XML to unified JSON for PUID={}", puid);
            String mt = messageType; // for lambda capture
            unifiedJson = transformTimer.recordCallable(() -> transformer.toUnifiedJson(xml, mt, puid));
            if (log.isDebugEnabled()) {
                log.debug("[TRANSFORM] Unified JSON (first 500 chars) PUID={} => {}", puid,
                        unifiedJson.substring(0, Math.min(500, unifiedJson.length())));
            }
        } catch (Exception txEx) {
            log.error("[TRANSFORM] Failed PUID={}, err={}", puid, txEx.getMessage());
            transformFailCounter.increment();
            kafkaPublisher.publishInvalid(puid, xml);
            try {
                if (inboundRepo != null) {
                    InboundMessage m = inboundRepo.findById(puid).orElse(null);
                    if (m != null) { m.setStatus("ERROR"); m.setError("TRANSFORM_FAIL"); inboundRepo.save(m); }
                }
            } catch (Exception ignore) { }
            return;
        }

        // Publish valid + best-effort persistence and event
        log.info("[KAFKA] Publishing valid message to topic payment-messages with key={}", puid);
        publishTimer.record(() -> kafkaPublisher.publishValid(puid, unifiedJson));
        log.info("[KAFKA] Publish complete for key={}", puid);
        try {
            if (unifiedRepo != null) {
                UnifiedMessage um = new UnifiedMessage();
                um.setPuid(puid);
                um.setMessageType(messageType);
                um.setCreatedAt(java.time.Instant.now());
                um.setJson(unifiedJson);
                unifiedRepo.save(um);
            }
        } catch (Exception e) {
            log.warn("[PERSIST] UnifiedMessage save failed PUID={}, err={}", puid, e.getMessage());
        }
        try { eventPublisher.publishPaymentReceivedEvent(puid, "G3I", "payment-messages"); } catch (Exception ignore) { }
        try {
            if (inboundRepo != null) {
                InboundMessage m = inboundRepo.findById(puid).orElse(null);
                if (m != null) { m.setStatus("PUBLISHED"); inboundRepo.save(m); }
            }
        } catch (Exception ignore) { }
    }

    private String safe(String s) {
        if (s == null) return "";
        return s.replace("\\", "\\\\").replace("\"", "\\\"");
    }
}


