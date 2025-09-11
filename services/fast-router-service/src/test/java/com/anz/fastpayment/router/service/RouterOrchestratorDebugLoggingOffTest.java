package com.anz.fastpayment.router.service;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import com.anz.fastpayment.router.repository.InboundMessageRepository;
import com.anz.fastpayment.router.repository.UnifiedMessageRepository;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.avro.Schema;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class RouterOrchestratorDebugLoggingOffTest {

    @Test
    void debugDisabled_skipsDebugBranch() throws Exception {
        Logger logger = (Logger) LoggerFactory.getLogger(RouterOrchestrator.class);
        Level original = logger.getLevel();
        logger.setLevel(Level.INFO);
        try {
            @SuppressWarnings("unchecked") ObjectProvider<InboundMessageRepository> inProv = (ObjectProvider<InboundMessageRepository>) mock(ObjectProvider.class);
            @SuppressWarnings("unchecked") ObjectProvider<UnifiedMessageRepository> unProv = (ObjectProvider<UnifiedMessageRepository>) mock(ObjectProvider.class);
            PuidGenerator puid = mock(PuidGenerator.class);
            when(puid.nextPuid()).thenReturn("PD");
            Iso20022MessageTypeDetector det = mock(Iso20022MessageTypeDetector.class);
            when(det.detectType(anyString())).thenReturn("pacs.003.001.11");
            XmlSchemaValidator v = mock(XmlSchemaValidator.class);
            Iso20022Transformer t = mock(Iso20022Transformer.class);
            when(t.toUnifiedJson(anyString(), anyString(), anyString())).thenReturn("{}{}");
            KafkaPublisher pub = mock(KafkaPublisher.class);
            Schema s = new Schema.Parser().parse("{\n  \"type\": \"record\",\n  \"name\": \"UnifiedPaymentMessage\",\n  \"fields\": [\n    { \"name\": \"messageType\", \"type\": \"string\" },\n    { \"name\": \"messageVersion\", \"type\": [\"null\", \"string\"], \"default\": null },\n    { \"name\": \"messageId\", \"type\": \"string\" },\n    { \"name\": \"creationDateTime\", \"type\": \"string\" },\n    { \"name\": \"supplementaryData\", \"type\": [\"null\", {\"type\": \"map\", \"values\": \"string\"}], \"default\": null }\n  ]\n}");
            when(pub.getUnifiedSchema()).thenReturn(s);
            EventPublisher ev = mock(EventPublisher.class);
            UniqueIdExtractor uid = mock(UniqueIdExtractor.class);
            DuplicateChecker dc = mock(DuplicateChecker.class);
            when(dc.isDuplicateAndRecord(anyString(), anyString(), anyString())).thenReturn(false);

            RouterOrchestrator r = new RouterOrchestrator(puid, inProv, det, v, t, pub, ev, uid, unProv, dc, new com.fasterxml.jackson.databind.ObjectMapper(), new SimpleMeterRegistry());
            assertDoesNotThrow(() -> r.processInboundXml("<x/>"));
        } finally {
            logger.setLevel(original);
        }
    }
}
