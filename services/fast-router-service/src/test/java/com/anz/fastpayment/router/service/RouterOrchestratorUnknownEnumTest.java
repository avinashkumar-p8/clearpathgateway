package com.anz.fastpayment.router.service;

import com.anz.fastpayment.router.repository.InboundMessageRepository;
import com.anz.fastpayment.router.repository.UnifiedMessageRepository;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.ObjectProvider;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class RouterOrchestratorUnknownEnumTest {

    @Test
    void unknownTypeCausesPublishEnumFailure() throws Exception {
        PuidGenerator puidGen = mock(PuidGenerator.class);
        when(puidGen.nextPuid()).thenReturn("PU");
        ObjectProvider<InboundMessageRepository> inProv = mock(ObjectProvider.class);
        ObjectProvider<UnifiedMessageRepository> unProv = mock(ObjectProvider.class);
        Iso20022MessageTypeDetector typeDetector = mock(Iso20022MessageTypeDetector.class);
        when(typeDetector.detectType(anyString())).thenReturn("unknown");
        XmlSchemaValidator validator = mock(XmlSchemaValidator.class);
        Iso20022Transformer transformer = mock(Iso20022Transformer.class);
        when(transformer.toUnifiedJson(anyString(), anyString(), anyString())).thenReturn("{}");
        KafkaPublisher publisher = spy(new KafkaPublisher(mock(org.springframework.kafka.core.KafkaTemplate.class), null));
        EventPublisher eventPublisher = mock(EventPublisher.class);
        UniqueIdExtractor uid = mock(UniqueIdExtractor.class);
        DuplicateChecker dc = mock(DuplicateChecker.class);
        when(dc.isDuplicateAndRecord(anyString(), anyString(), anyString())).thenReturn(false);

        RouterOrchestrator orch = new RouterOrchestrator(
                puidGen, inProv, typeDetector, validator, transformer,
                publisher, eventPublisher, uid, unProv, dc,
                new com.fasterxml.jackson.databind.ObjectMapper(), new SimpleMeterRegistry()
        );
        String xml = "<Document><x/></Document>";
        assertThrows(RuntimeException.class, () -> orch.processInboundXml(xml));
    }
}
