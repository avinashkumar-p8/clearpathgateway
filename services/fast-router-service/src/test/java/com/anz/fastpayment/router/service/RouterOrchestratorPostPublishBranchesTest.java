package com.anz.fastpayment.router.service;

import com.anz.fastpayment.router.repository.InboundMessageRepository;
import com.anz.fastpayment.router.repository.UnifiedMessageRepository;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.ObjectProvider;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class RouterOrchestratorPostPublishBranchesTest {

    @Test
    void unifiedRepoSaveFailureAndEventPublisherFailureAreIgnored() throws Exception {
        // repos
        InboundMessageRepository inboundRepo = mock(InboundMessageRepository.class);
        ObjectProvider<InboundMessageRepository> inboundProvider = mock(ObjectProvider.class);
        when(inboundProvider.getIfAvailable()).thenReturn(inboundRepo);
        UnifiedMessageRepository unifiedRepo = mock(UnifiedMessageRepository.class);
        doThrow(new RuntimeException("save boom")).when(unifiedRepo).save(any());
        ObjectProvider<UnifiedMessageRepository> unifiedProvider = mock(ObjectProvider.class);
        when(unifiedProvider.getIfAvailable()).thenReturn(unifiedRepo);

        // services
        PuidGenerator puidGen = mock(PuidGenerator.class);
        when(puidGen.nextPuid()).thenReturn("PUID-Z");
        Iso20022MessageTypeDetector typeDetector = mock(Iso20022MessageTypeDetector.class);
        when(typeDetector.detectType(anyString())).thenReturn("pacs.003.001.11");
        XmlSchemaValidator validator = mock(XmlSchemaValidator.class); // success path
        Iso20022Transformer transformer = mock(Iso20022Transformer.class);
        when(transformer.toUnifiedJson(anyString(), anyString(), anyString())).thenReturn("{\n}\n");
        KafkaPublisher kafkaPublisher = mock(KafkaPublisher.class);
        // minimal schema to construct avro record
        org.apache.avro.Schema schema = new org.apache.avro.Schema.Parser().parse("{\n  \"type\": \"record\",\n  \"name\": \"UnifiedPaymentMessage\",\n  \"fields\": [\n    { \"name\": \"messageType\", \"type\": {\"type\":\"enum\",\"name\":\"MessageType\",\"symbols\":[\"PACS_003\"]}},\n    { \"name\": \"messageVersion\", \"type\": [\"null\", \"string\"], \"default\": null },\n    { \"name\": \"messageId\", \"type\": \"string\" },\n    { \"name\": \"creationDateTime\", \"type\": \"string\" },\n    { \"name\": \"supplementaryData\", \"type\": [\"null\", {\"type\": \"map\", \"values\": \"string\"}], \"default\": null }\n  ]\n}");
        when(kafkaPublisher.getUnifiedSchema()).thenReturn(schema);
        // event publisher throws in final stage
        EventPublisher eventPublisher = mock(EventPublisher.class);
        doThrow(new RuntimeException("event boom")).when(eventPublisher).publishPaymentReceivedEvent(anyString(), anyString(), anyString());
        UniqueIdExtractor uid = mock(UniqueIdExtractor.class);
        DuplicateChecker dc = mock(DuplicateChecker.class);
        when(dc.isDuplicateAndRecord(anyString(), anyString(), anyString())).thenReturn(false);

        RouterOrchestrator orch = new RouterOrchestrator(
                puidGen, inboundProvider, typeDetector, validator, transformer,
                kafkaPublisher, eventPublisher, uid, unifiedProvider, dc,
                new com.fasterxml.jackson.databind.ObjectMapper(), new SimpleMeterRegistry()
        );

        assertDoesNotThrow(() -> orch.processInboundXml("<Document xmlns=\"urn:iso:std:iso:20022:tech:xsd:pacs.003.001.11\"><FIToFICstmrDrctDbt><GrpHdr><MsgId>M1</MsgId></GrpHdr></FIToFICstmrDrctDbt></Document>"));
        verify(kafkaPublisher).publishValidUnified(any(), eq("PUID-Z"));
        verify(unifiedRepo).save(any());
        verify(eventPublisher).publishPaymentReceivedEvent(eq("PUID-Z"), anyString(), anyString());
    }
}
