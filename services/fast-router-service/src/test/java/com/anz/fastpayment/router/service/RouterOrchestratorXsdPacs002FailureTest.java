package com.anz.fastpayment.router.service;

import com.anz.fastpayment.router.repository.InboundMessageRepository;
import com.anz.fastpayment.router.repository.UnifiedMessageRepository;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.ObjectProvider;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class RouterOrchestratorXsdPacs002FailureTest {

    @Test
    void xsdFailure_publishesInvalid_and_pacs002_publish_fails_is_caught() throws Exception {
        @SuppressWarnings("unchecked") ObjectProvider<InboundMessageRepository> inProv = (ObjectProvider<InboundMessageRepository>) mock(ObjectProvider.class);
        @SuppressWarnings("unchecked") ObjectProvider<UnifiedMessageRepository> unProv = (ObjectProvider<UnifiedMessageRepository>) mock(ObjectProvider.class);
        PuidGenerator puid = mock(PuidGenerator.class);
        when(puid.nextPuid()).thenReturn("PX");
        Iso20022MessageTypeDetector det = mock(Iso20022MessageTypeDetector.class);
        when(det.detectType(anyString())).thenReturn("pacs.003.001.11");
        XmlSchemaValidator v = mock(XmlSchemaValidator.class);
        doThrow(new IllegalArgumentException("XSD FAIL")).when(v).validate(anyString(), anyString());
        Iso20022Transformer t = mock(Iso20022Transformer.class);
        KafkaPublisher pub = mock(KafkaPublisher.class);
        // invalid publish ok
        doNothing().when(pub).publishInvalid(anyString(), anyString());
        // pacs002 publish throws and is caught
        doThrow(new RuntimeException("kafka down")).when(pub).publishPacs002Request(anyString(), anyString());
        EventPublisher ev = mock(EventPublisher.class);
        UniqueIdExtractor uid = mock(UniqueIdExtractor.class);
        when(uid.extractUniqueId(anyString(), anyString())).thenReturn("");
        DuplicateChecker dc = mock(DuplicateChecker.class);

        RouterOrchestrator r = new RouterOrchestrator(puid, inProv, det, v, t, pub, ev, uid, unProv, dc, new com.fasterxml.jackson.databind.ObjectMapper(), new SimpleMeterRegistry());
        assertDoesNotThrow(() -> r.processInboundXml("<x/>"));
        verify(pub).publishInvalid(eq("PX"), anyString());
        verify(pub).publishPacs002Request(eq("PX"), anyString());
    }
}
