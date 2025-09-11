package com.anz.fastpayment.router.service;

import com.anz.fastpayment.router.repository.InboundMessageRepository;
import com.anz.fastpayment.router.repository.UnifiedMessageRepository;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.ObjectProvider;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class RouterOrchestratorPublishFailureTest {

    @Test
    void wrapsPublishExceptionInRuntimeInsideTimer() throws Exception {
        @SuppressWarnings("unchecked")
        ObjectProvider<InboundMessageRepository> inboundProvider = (ObjectProvider<InboundMessageRepository>) mock(ObjectProvider.class);
        @SuppressWarnings("unchecked")
        ObjectProvider<UnifiedMessageRepository> unifiedProvider = (ObjectProvider<UnifiedMessageRepository>) mock(ObjectProvider.class);

        PuidGenerator puidGen = mock(PuidGenerator.class);
        when(puidGen.nextPuid()).thenReturn("PE");
        Iso20022MessageTypeDetector typeDetector = mock(Iso20022MessageTypeDetector.class);
        when(typeDetector.detectType(anyString())).thenReturn("pacs.003.001.11");
        XmlSchemaValidator validator = mock(XmlSchemaValidator.class);
        Iso20022Transformer transformer = mock(Iso20022Transformer.class);
        when(transformer.toUnifiedJson(anyString(), anyString(), anyString())).thenReturn("{}\n");
        KafkaPublisher kafkaPublisher = mock(KafkaPublisher.class);
        org.apache.avro.Schema schema = new org.apache.avro.Schema.Parser().parse("{\n  \"type\": \"record\",\n  \"name\": \"UnifiedPaymentMessage\",\n  \"fields\": [\n    { \"name\": \"messageType\", \"type\": \"string\" },\n    { \"name\": \"messageVersion\", \"type\": [\"null\", \"string\"], \"default\": null },\n    { \"name\": \"messageId\", \"type\": \"string\" },\n    { \"name\": \"creationDateTime\", \"type\": \"string\" },\n    { \"name\": \"supplementaryData\", \"type\": [\"null\", {\"type\": \"map\", \"values\": \"string\"}], \"default\": null }\n  ]\n}");
        when(kafkaPublisher.getUnifiedSchema()).thenReturn(schema);
        doThrow(new RuntimeException("publish fail")).when(kafkaPublisher).publishValidUnified(any(), anyString());
        EventPublisher eventPublisher = mock(EventPublisher.class);
        UniqueIdExtractor uid = mock(UniqueIdExtractor.class);
        DuplicateChecker dc = mock(DuplicateChecker.class);
        when(dc.isDuplicateAndRecord(anyString(), anyString(), anyString())).thenReturn(false);

        RouterOrchestrator orch = new RouterOrchestrator(
                puidGen, inboundProvider, typeDetector, validator, transformer,
                kafkaPublisher, eventPublisher, uid, unifiedProvider, dc,
                new com.fasterxml.jackson.databind.ObjectMapper(), new SimpleMeterRegistry()
        );

        assertThrows(RuntimeException.class, () -> orch.processInboundXml("<Document xmlns=\"urn:iso:std:iso:20022:tech:xsd:pacs.003.001.11\"><FIToFICstmrDrctDbt/></Document>"));
    }
}
