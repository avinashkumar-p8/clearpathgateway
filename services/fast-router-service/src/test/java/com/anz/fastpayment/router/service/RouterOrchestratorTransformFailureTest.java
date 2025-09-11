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

class RouterOrchestratorTransformFailureTest {

    @Test
    void transformFailure_publishesInvalid_and_updatesError() throws Exception {
        InboundMessageRepository inboundRepo = mock(InboundMessageRepository.class);
        InboundMessage stored = new InboundMessage();
        stored.setPuid("PT");
        when(inboundRepo.findById("PT")).thenReturn(Optional.of(stored));
        @SuppressWarnings("unchecked") ObjectProvider<InboundMessageRepository> inProv = (ObjectProvider<InboundMessageRepository>) mock(ObjectProvider.class);
        when(inProv.getIfAvailable()).thenReturn(inboundRepo);
        @SuppressWarnings("unchecked") ObjectProvider<UnifiedMessageRepository> unProv = (ObjectProvider<UnifiedMessageRepository>) mock(ObjectProvider.class);

        PuidGenerator puid = mock(PuidGenerator.class);
        when(puid.nextPuid()).thenReturn("PT");
        Iso20022MessageTypeDetector det = mock(Iso20022MessageTypeDetector.class);
        when(det.detectType(anyString())).thenReturn("pacs.003.001.11");
        XmlSchemaValidator v = mock(XmlSchemaValidator.class);
        Iso20022Transformer t = mock(Iso20022Transformer.class);
        when(t.toUnifiedJson(anyString(), anyString(), anyString())).thenThrow(new RuntimeException("tx"));
        KafkaPublisher pub = mock(KafkaPublisher.class);
        EventPublisher ev = mock(EventPublisher.class);
        UniqueIdExtractor uid = mock(UniqueIdExtractor.class);
        DuplicateChecker dc = mock(DuplicateChecker.class);
        when(dc.isDuplicateAndRecord(anyString(), anyString(), anyString())).thenReturn(false);

        RouterOrchestrator r = new RouterOrchestrator(puid, inProv, det, v, t, pub, ev, uid, unProv, dc, new com.fasterxml.jackson.databind.ObjectMapper(), new SimpleMeterRegistry());
        assertDoesNotThrow(() -> r.processInboundXml("<x/>"));
        verify(pub).publishInvalid(eq("PT"), anyString());
        verify(inboundRepo, atLeastOnce()).save(argThat(m -> "ERROR".equals(((InboundMessage)m).getStatus())));
    }
}
