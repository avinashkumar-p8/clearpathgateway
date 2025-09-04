package com.anz.fastpayment.router.service;

import com.anz.fastpayment.router.repository.InboundMessageRepository;
import com.anz.fastpayment.router.repository.UnifiedMessageRepository;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.avro.Schema;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.ObjectProvider;

import static org.mockito.Mockito.*;

class RouterOrchestratorAllTypesTest {

    private Schema enumUnifiedSchema;

    @BeforeEach
    void initSchema() {
        enumUnifiedSchema = new Schema.Parser().parse("{\n  \"type\": \"record\",\n  \"name\": \"UnifiedPaymentMessage\",\n  \"fields\": [\n    { \"name\": \"messageType\", \"type\": {\"type\":\"enum\",\"name\":\"MessageType\",\"symbols\":[\"PACS_008\",\"PACS_003\",\"PACS_007\",\"CAMT_056\",\"PACS_002\",\"CAMT_029\",\"UNKNOWN\"]}},\n    { \"name\": \"messageVersion\", \"type\": [\"null\", \"string\"], \"default\": null },\n    { \"name\": \"messageId\", \"type\": \"string\" },\n    { \"name\": \"creationDateTime\", \"type\": \"string\" },\n    { \"name\": \"supplementaryData\", \"type\": [\"null\", {\"type\": \"map\", \"values\": \"string\"}], \"default\": null }\n  ]\n}");
    }

    private RouterOrchestrator buildOrchestrator(String messageType,
                                                 boolean xsdPasses,
                                                 boolean duplicate,
                                                 KafkaPublisher kafkaPublisher,
                                                 UniqueIdExtractor uniqueIdExtractor,
                                                 Iso20022Transformer transformer) {
        PuidGenerator puid = mock(PuidGenerator.class);
        when(puid.nextPuid()).thenReturn("PUID-TEST");
        @SuppressWarnings("unchecked")
        ObjectProvider<InboundMessageRepository> inboundProvider = (ObjectProvider<InboundMessageRepository>) mock(ObjectProvider.class);
        @SuppressWarnings("unchecked")
        ObjectProvider<UnifiedMessageRepository> unifiedProvider = (ObjectProvider<UnifiedMessageRepository>) mock(ObjectProvider.class);

        Iso20022MessageTypeDetector detector = mock(Iso20022MessageTypeDetector.class);
        when(detector.detectType(anyString())).thenReturn(messageType);

        XmlSchemaValidator validator = mock(XmlSchemaValidator.class);
        if (!xsdPasses) {
            doThrow(new IllegalArgumentException("XSD FAIL")).when(validator).validate(anyString(), anyString());
        }

        DuplicateChecker duplicateChecker = mock(DuplicateChecker.class);
        when(duplicateChecker.isDuplicateAndRecord(anyString(), anyString(), anyString())).thenReturn(duplicate);

        EventPublisher eventPublisher = mock(EventPublisher.class);
        com.fasterxml.jackson.databind.ObjectMapper om = new com.fasterxml.jackson.databind.ObjectMapper();

        return new RouterOrchestrator(
                puid,
                inboundProvider,
                detector,
                validator,
                transformer,
                kafkaPublisher,
                eventPublisher,
                uniqueIdExtractor,
                unifiedProvider,
                duplicateChecker,
                om,
                new SimpleMeterRegistry()
        );
    }

    private KafkaPublisher stubPublisher() {
        KafkaPublisher publisher = mock(KafkaPublisher.class);
        when(publisher.getUnifiedSchema()).thenReturn(enumUnifiedSchema);
        return publisher;
    }

    private UniqueIdExtractor stubUniqueId(String uniqueId) {
        UniqueIdExtractor extractor = mock(UniqueIdExtractor.class);
        when(extractor.extractUniqueId(anyString(), anyString())).thenReturn(uniqueId);
        return extractor;
    }

    private Iso20022Transformer stubTransformer() {
        Iso20022Transformer transformer = mock(Iso20022Transformer.class);
        try {
            when(transformer.toUnifiedJson(anyString(), anyString(), anyString()))
                    .thenReturn("{\"ok\":true}");
        } catch (Exception ignore) { }
        return transformer;
    }

    // Valid flows
    @Test
    void pacs003_valid_publishesAvro() {
        KafkaPublisher publisher = stubPublisher();
        UniqueIdExtractor extractor = stubUniqueId("INSTR-1");
        Iso20022Transformer transformer = stubTransformer();
        RouterOrchestrator orch = buildOrchestrator("pacs.003.001.11", true, false, publisher, extractor, transformer);
        orch.processInboundXml("<xml/>");
        verify(publisher, times(1)).publishValidUnified(any(), eq("PUID-TEST"));
    }

    @Test
    void pacs007_valid_publishesAvro() {
        KafkaPublisher publisher = stubPublisher();
        UniqueIdExtractor extractor = stubUniqueId("INSTR-2");
        Iso20022Transformer transformer = stubTransformer();
        RouterOrchestrator orch = buildOrchestrator("pacs.007.001.13", true, false, publisher, extractor, transformer);
        orch.processInboundXml("<xml/>");
        verify(publisher, times(1)).publishValidUnified(any(), eq("PUID-TEST"));
    }

    @Test
    void camt056_valid_publishesAvro() {
        KafkaPublisher publisher = stubPublisher();
        UniqueIdExtractor extractor = stubUniqueId("ORIG-1");
        Iso20022Transformer transformer = stubTransformer();
        RouterOrchestrator orch = buildOrchestrator("camt.056.001.11", true, false, publisher, extractor, transformer);
        orch.processInboundXml("<xml/>");
        verify(publisher, times(1)).publishValidUnified(any(), eq("PUID-TEST"));
    }

    // Invalid flows -> exception + pacs002
    @Test
    void pacs003_invalid_emitsExceptionAndPacs002() {
        KafkaPublisher publisher = stubPublisher();
        UniqueIdExtractor extractor = stubUniqueId("INSTR-1");
        Iso20022Transformer transformer = stubTransformer();
        RouterOrchestrator orch = buildOrchestrator("pacs.003.001.11", false, false, publisher, extractor, transformer);
        orch.processInboundXml("<xml/>");
        verify(publisher, times(1)).publishInvalid(eq("PUID-TEST"), anyString());
        verify(publisher, times(1)).publishPacs002Request(eq("PUID-TEST"), anyString());
    }

    @Test
    void pacs007_invalid_emitsExceptionAndPacs002() {
        KafkaPublisher publisher = stubPublisher();
        UniqueIdExtractor extractor = stubUniqueId("INSTR-2");
        Iso20022Transformer transformer = stubTransformer();
        RouterOrchestrator orch = buildOrchestrator("pacs.007.001.13", false, false, publisher, extractor, transformer);
        orch.processInboundXml("<xml/>");
        verify(publisher, times(1)).publishInvalid(eq("PUID-TEST"), anyString());
        verify(publisher, times(1)).publishPacs002Request(eq("PUID-TEST"), anyString());
    }

    @Test
    void camt056_invalid_emitsExceptionAndPacs002() {
        KafkaPublisher publisher = stubPublisher();
        UniqueIdExtractor extractor = stubUniqueId("ORIG-1");
        Iso20022Transformer transformer = stubTransformer();
        RouterOrchestrator orch = buildOrchestrator("camt.056.001.11", false, false, publisher, extractor, transformer);
        orch.processInboundXml("<xml/>");
        verify(publisher, times(1)).publishInvalid(eq("PUID-TEST"), anyString());
        verify(publisher, times(1)).publishPacs002Request(eq("PUID-TEST"), anyString());
    }

    // Duplicate flows -> early return, no publishValid
    @Test
    void pacs003_duplicate_skipsPublish() {
        KafkaPublisher publisher = stubPublisher();
        UniqueIdExtractor extractor = stubUniqueId("INSTR-1");
        Iso20022Transformer transformer = stubTransformer();
        RouterOrchestrator orch = buildOrchestrator("pacs.003.001.11", true, true, publisher, extractor, transformer);
        orch.processInboundXml("<xml/>");
        verify(publisher, never()).publishValidUnified(any(), anyString());
    }

    @Test
    void pacs007_duplicate_skipsPublish() {
        KafkaPublisher publisher = stubPublisher();
        UniqueIdExtractor extractor = stubUniqueId("INSTR-2");
        Iso20022Transformer transformer = stubTransformer();
        RouterOrchestrator orch = buildOrchestrator("pacs.007.001.13", true, true, publisher, extractor, transformer);
        orch.processInboundXml("<xml/>");
        verify(publisher, never()).publishValidUnified(any(), anyString());
    }

    @Test
    void camt056_duplicate_skipsPublish() {
        KafkaPublisher publisher = stubPublisher();
        UniqueIdExtractor extractor = stubUniqueId("ORIG-1");
        Iso20022Transformer transformer = stubTransformer();
        RouterOrchestrator orch = buildOrchestrator("camt.056.001.11", true, true, publisher, extractor, transformer);
        orch.processInboundXml("<xml/>");
        verify(publisher, never()).publishValidUnified(any(), anyString());
    }
}


