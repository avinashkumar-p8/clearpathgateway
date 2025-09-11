package com.anz.fastpayment.router.service;

import com.anz.fastpayment.router.repository.InboundMessageRepository;
import com.anz.fastpayment.router.repository.UnifiedMessageRepository;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.avro.Schema;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.ObjectProvider;

import static org.mockito.Mockito.*;

class RouterOrchestratorMoreBranchesTest {

    private RouterOrchestrator build(PuidGenerator pg, Iso20022MessageTypeDetector det) {
        @SuppressWarnings("unchecked") ObjectProvider<InboundMessageRepository> inProv = (ObjectProvider<InboundMessageRepository>) mock(ObjectProvider.class);
        @SuppressWarnings("unchecked") ObjectProvider<UnifiedMessageRepository> unProv = (ObjectProvider<UnifiedMessageRepository>) mock(ObjectProvider.class);
        XmlSchemaValidator v = new XmlSchemaValidator();
        org.springframework.test.util.ReflectionTestUtils.setField(v, "xsdValidationEnabled", true);
        org.springframework.test.util.ReflectionTestUtils.setField(v, "testBypassValidation", true);
        return new RouterOrchestrator(
                pg,
                inProv,
                det,
                v,
                mock(Iso20022Transformer.class),
                mock(KafkaPublisher.class),
                mock(EventPublisher.class),
                new UniqueIdExtractor(),
                unProv,
                mock(DuplicateChecker.class),
                new com.fasterxml.jackson.databind.ObjectMapper(),
                new SimpleMeterRegistry()
        );
    }

    @Test
    void blankXmlIsIgnoredEarly() {
        PuidGenerator pg = mock(PuidGenerator.class);
        when(pg.nextPuid()).thenReturn("PX");
        Iso20022MessageTypeDetector det = mock(Iso20022MessageTypeDetector.class);
        RouterOrchestrator r = build(pg, det);
        r.processInboundXml("   ");
        verifyNoInteractions(det);
    }

    @Test
    void enumMappingAndVersionPathsCovered() {
        PuidGenerator pg = mock(PuidGenerator.class);
        when(pg.nextPuid()).thenReturn("PX");
        Iso20022MessageTypeDetector det = mock(Iso20022MessageTypeDetector.class);
        // invoke through public flow with different types to exercise mapToAvroEnum/extractVersion paths
        RouterOrchestrator r = build(pg, det);
        for (String mt : new String[]{"pacs.008.001.13","pacs.003.001.11","pacs.007.001.13","camt.056.001.11","pacs.002.001.15","camt.029.001.13","head.001.001.01","unknown"}) {
            when(det.detectType(anyString())).thenReturn(mt);
            r.processInboundXml("<x/>");
        }
    }
}
