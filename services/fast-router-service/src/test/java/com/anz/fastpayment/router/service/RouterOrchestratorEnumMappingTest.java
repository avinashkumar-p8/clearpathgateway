package com.anz.fastpayment.router.service;

import com.anz.fastpayment.router.repository.InboundMessageRepository;
import com.anz.fastpayment.router.repository.UnifiedMessageRepository;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.kafka.core.KafkaTemplate;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class RouterOrchestratorEnumMappingTest {

    private RouterOrchestrator newOrchestrator(String type, KafkaPublisher kafkaPublisher) throws Exception {
        PuidGenerator puidGen = mock(PuidGenerator.class);
        when(puidGen.nextPuid()).thenReturn("PZ");
        ObjectProvider<InboundMessageRepository> inProv = mock(ObjectProvider.class);
        ObjectProvider<UnifiedMessageRepository> unProv = mock(ObjectProvider.class);
        Iso20022MessageTypeDetector typeDetector = mock(Iso20022MessageTypeDetector.class);
        when(typeDetector.detectType(anyString())).thenReturn(type);
        XmlSchemaValidator validator = mock(XmlSchemaValidator.class);
        Iso20022Transformer transformer = mock(Iso20022Transformer.class);
        when(transformer.toUnifiedJson(anyString(), anyString(), anyString())).thenReturn("{}");
        EventPublisher eventPublisher = mock(EventPublisher.class);
        UniqueIdExtractor uid = mock(UniqueIdExtractor.class);
        DuplicateChecker dc = mock(DuplicateChecker.class);
        when(dc.isDuplicateAndRecord(anyString(), anyString(), anyString())).thenReturn(false);
        return new RouterOrchestrator(
                puidGen, inProv, typeDetector, validator, transformer,
                kafkaPublisher, eventPublisher, uid, unProv, dc,
                new com.fasterxml.jackson.databind.ObjectMapper(), new SimpleMeterRegistry()
        );
    }

    private void assertMap(String messageType, String expectedEnum, String expectedVersion) throws Exception {
        @SuppressWarnings("unchecked")
        KafkaTemplate<String, org.apache.avro.generic.GenericRecord> avroTemplate = mock(KafkaTemplate.class);
        // Avoid blocking get() inside publisher
        when(avroTemplate.send(anyString(), anyString(), any())).thenReturn(java.util.concurrent.CompletableFuture.completedFuture(null));
        KafkaPublisher realPublisher = spy(new KafkaPublisher(avroTemplate, null));
        ArgumentCaptor<GenericRecord> recCap = ArgumentCaptor.forClass(GenericRecord.class);
        doAnswer(i -> null).when(realPublisher).publishValidUnified(recCap.capture(), anyString());

        RouterOrchestrator orch = newOrchestrator(messageType, realPublisher);
        String xml = "<Document xmlns=\"urn:iso:std:iso:20022:tech:xsd:" + messageType + "\"><x/></Document>";
        orch.processInboundXml(xml);
        GenericRecord rec = recCap.getValue();
        assertEquals(expectedEnum, rec.get("messageType").toString());
        assertEquals(expectedVersion, rec.get("messageVersion"));
    }

    @Test
    void mapsPacs003() throws Exception { assertMap("pacs.003.001.11", "PACS_003", "11"); }

    @Test
    void mapsPacs007() throws Exception { assertMap("pacs.007.001.13", "PACS_007", "13"); }

    @Test
    void mapsPacs008() throws Exception { assertMap("pacs.008.001.13", "PACS_008", "13"); }

    @Test
    void mapsCamt056() throws Exception { assertMap("camt.056.001.11", "CAMT_056", "11"); }

    @Test
    void mapsHead001() throws Exception { assertMap("head.001.001.01", "HEAD_001", "01"); }

    @Test
    void mapsPacs002() throws Exception { assertMap("pacs.002.001.15", "PACS_002", "15"); }

    @Test
    void mapsCamt029() throws Exception { assertMap("camt.029.001.13", "CAMT_029", "13"); }
}
