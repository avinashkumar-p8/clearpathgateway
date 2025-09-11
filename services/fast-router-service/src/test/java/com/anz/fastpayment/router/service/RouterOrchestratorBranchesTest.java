package com.anz.fastpayment.router.service;

import com.anz.fastpayment.router.model.InboundMessage;
import com.anz.fastpayment.router.repository.InboundMessageRepository;
import com.anz.fastpayment.router.repository.UnifiedMessageRepository;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.ObjectProvider;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class RouterOrchestratorBranchesTest {

    @Test
    void persistsInboundAndUnified_whenReposPresent() throws Exception {
        PuidGenerator puidGen = mock(PuidGenerator.class);
        when(puidGen.nextPuid()).thenReturn("PUID-TEST");
        InboundMessageRepository inboundRepo = mock(InboundMessageRepository.class);
        ObjectProvider<InboundMessageRepository> inboundProvider = mock(ObjectProvider.class);
        when(inboundProvider.getIfAvailable()).thenReturn(inboundRepo);
        UnifiedMessageRepository unifiedRepo = mock(UnifiedMessageRepository.class);
        ObjectProvider<UnifiedMessageRepository> unifiedProvider = mock(ObjectProvider.class);
        when(unifiedProvider.getIfAvailable()).thenReturn(unifiedRepo);
        Iso20022MessageTypeDetector typeDetector = mock(Iso20022MessageTypeDetector.class);
        when(typeDetector.detectType(anyString())).thenReturn("pacs.003.001.11");
        XmlSchemaValidator validator = mock(XmlSchemaValidator.class); // passes
        Iso20022Transformer transformer = mock(Iso20022Transformer.class);
        when(transformer.toUnifiedJson(anyString(), anyString(), anyString())).thenReturn("{}\n");
        KafkaPublisher kafkaPublisher = mock(KafkaPublisher.class);
        org.apache.avro.Schema schema = new org.apache.avro.Schema.Parser().parse("{\n  \"type\": \"record\",\n  \"name\": \"UnifiedPaymentMessage\",\n  \"fields\": [\n    { \"name\": \"messageType\", \"type\": {\"type\":\"enum\",\"name\":\"MessageType\",\"symbols\":[\"PACS_003\"]}},\n    { \"name\": \"messageVersion\", \"type\": [\"null\", \"string\"], \"default\": null },\n    { \"name\": \"messageId\", \"type\": \"string\" },\n    { \"name\": \"creationDateTime\", \"type\": \"string\" },\n    { \"name\": \"supplementaryData\", \"type\": [\"null\", {\"type\": \"map\", \"values\": \"string\"}], \"default\": null }\n  ]\n}");
        when(kafkaPublisher.getUnifiedSchema()).thenReturn(schema);
        EventPublisher eventPublisher = mock(EventPublisher.class);
        UniqueIdExtractor uid = mock(UniqueIdExtractor.class);
        when(uid.extractUniqueId(anyString(), anyString())).thenReturn(null);
        DuplicateChecker dc = mock(DuplicateChecker.class);
        when(dc.isDuplicateAndRecord(anyString(), anyString(), anyString())).thenReturn(false);

        RouterOrchestrator orch = new RouterOrchestrator(
                puidGen, inboundProvider, typeDetector, validator, transformer,
                kafkaPublisher, eventPublisher, uid, unifiedProvider, dc,
                new com.fasterxml.jackson.databind.ObjectMapper(), new SimpleMeterRegistry()
        );

        orch.processInboundXml("<Document xmlns=\"urn:iso:std:iso:20022:tech:xsd:pacs.003.001.11\"><FIToFICstmrDrctDbt><GrpHdr><MsgId>M1</MsgId></GrpHdr></FIToFICstmrDrctDbt></Document>");

        verify(inboundRepo, atLeastOnce()).save(any(InboundMessage.class));
        verify(unifiedRepo, atLeastOnce()).save(any());
        verify(eventPublisher, atLeastOnce()).publishPaymentReceivedEvent(eq("PUID-TEST"), anyString(), anyString());
        verify(kafkaPublisher, atLeastOnce()).publishValidUnified(any(), eq("PUID-TEST"));
    }

    @Test
    void publishExceptionIsCaught() throws Exception {
        PuidGenerator puidGen = mock(PuidGenerator.class);
        when(puidGen.nextPuid()).thenReturn("PUID-TEST");
        ObjectProvider<InboundMessageRepository> inboundProvider = mock(ObjectProvider.class);
        ObjectProvider<UnifiedMessageRepository> unifiedProvider = mock(ObjectProvider.class);
        Iso20022MessageTypeDetector typeDetector = mock(Iso20022MessageTypeDetector.class);
        when(typeDetector.detectType(anyString())).thenReturn("pacs.003.001.11");
        XmlSchemaValidator validator = mock(XmlSchemaValidator.class);
        Iso20022Transformer transformer = mock(Iso20022Transformer.class);
        when(transformer.toUnifiedJson(anyString(), anyString(), anyString())).thenReturn("{}\n");
        KafkaPublisher kafkaPublisher = mock(KafkaPublisher.class);
        org.apache.avro.Schema schema = new org.apache.avro.Schema.Parser().parse("{\n  \"type\": \"record\",\n  \"name\": \"UnifiedPaymentMessage\",\n  \"fields\": [\n    { \"name\": \"messageType\", \"type\": {\"type\":\"enum\",\"name\":\"MessageType\",\"symbols\":[\"PACS_003\"]}},\n    { \"name\": \"messageVersion\", \"type\": [\"null\", \"string\"], \"default\": null },\n    { \"name\": \"messageId\", \"type\": \"string\" },\n    { \"name\": \"creationDateTime\", \"type\": \"string\" },\n    { \"name\": \"supplementaryData\", \"type\": [\"null\", {\"type\": \"map\", \"values\": \"string\"}], \"default\": null }\n  ]\n}");
        when(kafkaPublisher.getUnifiedSchema()).thenReturn(schema);
        doThrow(new RuntimeException("publish boom")).when(kafkaPublisher).publishValidUnified(any(), anyString());
        EventPublisher eventPublisher = mock(EventPublisher.class);
        UniqueIdExtractor uid = mock(UniqueIdExtractor.class);
        DuplicateChecker dc = mock(DuplicateChecker.class);
        when(dc.isDuplicateAndRecord(anyString(), anyString(), anyString())).thenReturn(false);
        RouterOrchestrator orch = new RouterOrchestrator(
                puidGen, inboundProvider, typeDetector, validator, transformer,
                kafkaPublisher, eventPublisher, uid, unifiedProvider, dc,
                new com.fasterxml.jackson.databind.ObjectMapper(), new SimpleMeterRegistry()
        );
        assertThrows(RuntimeException.class, () ->
                orch.processInboundXml("<Document xmlns=\"urn:iso:std:iso:20022:tech:xsd:pacs.003.001.11\"><FIToFICstmrDrctDbt><GrpHdr><MsgId>M1</MsgId></GrpHdr></FIToFICstmrDrctDbt></Document>")
        );
    }
}
