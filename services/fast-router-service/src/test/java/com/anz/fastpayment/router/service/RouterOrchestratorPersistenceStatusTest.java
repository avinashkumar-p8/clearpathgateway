package com.anz.fastpayment.router.service;

import com.anz.fastpayment.router.model.InboundMessage;
import com.anz.fastpayment.router.repository.InboundMessageRepository;
import com.anz.fastpayment.router.repository.UnifiedMessageRepository;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.ObjectProvider;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class RouterOrchestratorPersistenceStatusTest {

    @Test
    void validatesAndPublishesUpdatesStatuses() throws Exception {
        InboundMessageRepository inboundRepo = mock(InboundMessageRepository.class);
        ObjectProvider<InboundMessageRepository> inboundProvider = mock(ObjectProvider.class);
        when(inboundProvider.getIfAvailable()).thenReturn(inboundRepo);
        UnifiedMessageRepository unifiedRepo = mock(UnifiedMessageRepository.class);
        ObjectProvider<UnifiedMessageRepository> unifiedProvider = mock(ObjectProvider.class);
        when(unifiedProvider.getIfAvailable()).thenReturn(unifiedRepo);

        PuidGenerator puidGen = mock(PuidGenerator.class);
        when(puidGen.nextPuid()).thenReturn("PV");
        InboundMessage stored = new InboundMessage();
        stored.setPuid("PV");
        when(inboundRepo.findById(eq("PV"))).thenReturn(Optional.of(stored));

        Iso20022MessageTypeDetector typeDetector = mock(Iso20022MessageTypeDetector.class);
        when(typeDetector.detectType(anyString())).thenReturn("pacs.003.001.11");
        XmlSchemaValidator validator = mock(XmlSchemaValidator.class);
        Iso20022Transformer transformer = mock(Iso20022Transformer.class);
        when(transformer.toUnifiedJson(anyString(), anyString(), anyString())).thenReturn("{}\n");
        KafkaPublisher kafkaPublisher = mock(KafkaPublisher.class);
        org.apache.avro.Schema schema = new org.apache.avro.Schema.Parser().parse("{\n  \"type\": \"record\",\n  \"name\": \"UnifiedPaymentMessage\",\n  \"fields\": [\n    { \"name\": \"messageType\", \"type\": {\"type\":\"enum\",\"name\":\"MessageType\",\"symbols\":[\"PACS_003\"]}},\n    { \"name\": \"messageVersion\", \"type\": [\"null\", \"string\"], \"default\": null },\n    { \"name\": \"messageId\", \"type\": \"string\" },\n    { \"name\": \"creationDateTime\", \"type\": \"string\" },\n    { \"name\": \"supplementaryData\", \"type\": [\"null\", {\"type\": \"map\", \"values\": \"string\"}], \"default\": null }\n  ]\n}");
        when(kafkaPublisher.getUnifiedSchema()).thenReturn(schema);
        EventPublisher eventPublisher = mock(EventPublisher.class);
        UniqueIdExtractor uid = mock(UniqueIdExtractor.class);
        DuplicateChecker dc = mock(DuplicateChecker.class);
        when(dc.isDuplicateAndRecord(anyString(), anyString(), anyString())).thenReturn(false);

        RouterOrchestrator orch = new RouterOrchestrator(
                puidGen, inboundProvider, typeDetector, validator, transformer,
                kafkaPublisher, eventPublisher, uid, unifiedProvider, dc,
                new com.fasterxml.jackson.databind.ObjectMapper(), new SimpleMeterRegistry()
        );

        assertDoesNotThrow(() -> orch.processInboundXml("<Document xmlns=\"urn:iso:std:iso:20022:tech:xsd:pacs.003.001.11\"><FIToFICstmrDrctDbt><GrpHdr><MsgId>M1</MsgId></GrpHdr></FIToFICstmrDrctDbt></Document>"));
        verify(inboundRepo, atLeast(3)).save(any(InboundMessage.class)); // RECEIVED, VALIDATED, PUBLISHED
        verify(unifiedRepo).save(any());
        verify(kafkaPublisher).publishValidUnified(any(), eq("PV"));
    }

    @Test
    void xsdFailureSetsErrorStatus() {
        InboundMessageRepository inboundRepo = mock(InboundMessageRepository.class);
        ObjectProvider<InboundMessageRepository> inboundProvider = mock(ObjectProvider.class);
        when(inboundProvider.getIfAvailable()).thenReturn(inboundRepo);
        InboundMessage m = new InboundMessage(); m.setPuid("PX");
        when(inboundRepo.findById(eq("PX"))).thenReturn(Optional.of(m));
        ObjectProvider<UnifiedMessageRepository> unifiedProvider = mock(ObjectProvider.class);

        PuidGenerator puidGen = mock(PuidGenerator.class);
        when(puidGen.nextPuid()).thenReturn("PX");
        Iso20022MessageTypeDetector typeDetector = mock(Iso20022MessageTypeDetector.class);
        when(typeDetector.detectType(anyString())).thenReturn("pacs.008.001.13");
        XmlSchemaValidator validator = mock(XmlSchemaValidator.class);
        doThrow(new IllegalArgumentException("XSD FAIL")).when(validator).validate(anyString(), anyString());
        Iso20022Transformer transformer = mock(Iso20022Transformer.class);
        KafkaPublisher kafkaPublisher = mock(KafkaPublisher.class);
        EventPublisher eventPublisher = mock(EventPublisher.class);
        UniqueIdExtractor uid = mock(UniqueIdExtractor.class);
        DuplicateChecker dc = mock(DuplicateChecker.class);

        RouterOrchestrator orch = new RouterOrchestrator(
                puidGen, inboundProvider, typeDetector, validator, transformer,
                kafkaPublisher, eventPublisher, uid, unifiedProvider, dc,
                new com.fasterxml.jackson.databind.ObjectMapper(), new SimpleMeterRegistry()
        );
        assertDoesNotThrow(() -> orch.processInboundXml("<x/>"));
        verify(inboundRepo, atLeast(2)).save(any(InboundMessage.class)); // RECEIVED, ERROR
        verify(kafkaPublisher).publishInvalid(eq("PX"), anyString());
    }
}
