package com.anz.fastpayment.router.service;

import com.anz.fastpayment.router.repository.InboundMessageRepository;
import com.anz.fastpayment.router.repository.UnifiedMessageRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;
import org.springframework.beans.factory.ObjectProvider;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;

class RouterOrchestratorTest {

    private PuidGenerator puidGenerator;
    private Iso20022MessageTypeDetector detector;
    private XmlSchemaValidator xsdValidator;
    private Iso20022Transformer transformer;
    private KafkaPublisher publisher;
    private String capturedKey;
    private String capturedPayload;

    private RouterOrchestrator orchestrator;

    @BeforeEach
    void setup() {
        // Deterministic PUID without Redis
        puidGenerator = new PuidGenerator() {
            @Override
            public String nextPuid() {
                return "G3I0000000000001";
            }
        };
        detector = new Iso20022MessageTypeDetector();
        xsdValidator = new XmlSchemaValidator();
        transformer = new Iso20022Transformer(new com.anz.fastpayment.router.mapping.TransformationConfigLoader(new com.fasterxml.jackson.databind.ObjectMapper()));
        publisher = new KafkaPublisher(null) {
            @Override
            public void publishValid(String key, String payload) {
                capturedKey = key;
                capturedPayload = payload;
            }
            @Override
            public void publishInvalid(String key, String payload) {
                capturedKey = key;
                capturedPayload = payload;
            }
        };
        ObjectProvider<com.anz.fastpayment.router.repository.InboundMessageRepository> provider = new ObjectProvider<>() {
            @Override public com.anz.fastpayment.router.repository.InboundMessageRepository getObject(Object... args) { return null; }
            @Override public com.anz.fastpayment.router.repository.InboundMessageRepository getIfAvailable() { return null; }
            @Override public com.anz.fastpayment.router.repository.InboundMessageRepository getIfUnique() { return null; }
            @Override public com.anz.fastpayment.router.repository.InboundMessageRepository getObject() { return null; }
            @Override public void forEach(java.util.function.Consumer action) { }
            @Override public java.util.stream.Stream stream() { return java.util.stream.Stream.empty(); }
            @Override public java.util.Iterator iterator() { return java.util.Collections.emptyIterator(); }
        };
        EventPublisher eventPublisher = mock(EventPublisher.class);
        UniqueIdExtractor uniqueIdExtractor = new UniqueIdExtractor();
        ObjectProvider<UnifiedMessageRepository> unifiedProvider = new ObjectProvider<>() {
            @Override public UnifiedMessageRepository getObject(Object... args) { return null; }
            @Override public UnifiedMessageRepository getIfAvailable() { return null; }
            @Override public UnifiedMessageRepository getIfUnique() { return null; }
            @Override public UnifiedMessageRepository getObject() { return null; }
            @Override public void forEach(java.util.function.Consumer action) { }
            @Override public java.util.stream.Stream stream() { return java.util.stream.Stream.empty(); }
            @Override public java.util.Iterator iterator() { return java.util.Collections.emptyIterator(); }
        };
        DuplicateChecker duplicateChecker = mock(DuplicateChecker.class);
        when(duplicateChecker.isDuplicateAndRecord(anyString(), anyString(), anyString())).thenReturn(false);
        orchestrator = new RouterOrchestrator(puidGenerator, provider, detector, xsdValidator, transformer, publisher, eventPublisher, uniqueIdExtractor, unifiedProvider, duplicateChecker, new SimpleMeterRegistry());
    }

    @Test
    void happyPath_shouldValidateTransformAndPublish() {
        String xml = """
                <Document xmlns=\"urn:iso:std:iso:20022:tech:xsd:pacs.008.001.13\">
                  <FIToFICstmrCdtTrf>
                    <GrpHdr>
                      <MsgId>MSG-001</MsgId>
                      <CreDtTm>2024-01-15T10:30:00Z</CreDtTm>
                      <NbOfTxs>1</NbOfTxs>
                      <CtrlSum>1000.00</CtrlSum>
                      <TtlIntrBkSttlmAmt Ccy=\"SGD\">1000.00</TtlIntrBkSttlmAmt>
                      <IntrBkSttlmDt>2024-01-15</IntrBkSttlmDt>
                      <SttlmInf><SttlmMtd>CLRG</SttlmMtd></SttlmInf>
                    </GrpHdr>
                    <CdtTrfTxInf>
                      <PmtId><EndToEndId>E2E-001</EndToEndId></PmtId>
                      <IntrBkSttlmAmt Ccy=\"SGD\">1000.00</IntrBkSttlmAmt>
                      <ChrgBr>SHAR</ChrgBr>
                      <Dbtr><Nm>John Doe</Nm></Dbtr>
                      <DbtrAcct><Id><Othr><Id>123456789</Id></Othr></Id></DbtrAcct>
                      <DbtrAgt><FinInstnId><BICFI>AAAAUS33</BICFI></FinInstnId></DbtrAgt>
                      <CdtrAgt><FinInstnId><BICFI>BBBBUS33</BICFI></FinInstnId></CdtrAgt>
                      <Cdtr><Nm>Jane Smith</Nm></Cdtr>
                      <CdtrAcct><Id><Othr><Id>987654321</Id></Othr></Id></CdtrAcct>
                    </CdtTrfTxInf>
                  </FIToFICstmrCdtTrf>
                </Document>
                """;

        orchestrator.processInboundXml(xml);

        assertTrue(capturedKey.equals("G3I0000000000001"));
        assertTrue(capturedPayload.contains("\"puid\":\"G3I0000000000001\""));
        assertTrue(capturedPayload.contains("\"messageType\":\"PACS_008\""));
    }

    @Test
    void xsdFailure_shouldPublishInvalid() {
        String xml = "<root/>"; // unknown type -> validator throws
        // New orchestrator with different deterministic PUID
        puidGenerator = new PuidGenerator() {
            @Override
            public String nextPuid() {
                return "G3I0000000000002";
            }
        };
        ObjectProvider<com.anz.fastpayment.router.repository.InboundMessageRepository> provider2 = new ObjectProvider<>() {
            @Override public com.anz.fastpayment.router.repository.InboundMessageRepository getObject(Object... args) { return null; }
            @Override public com.anz.fastpayment.router.repository.InboundMessageRepository getIfAvailable() { return null; }
            @Override public com.anz.fastpayment.router.repository.InboundMessageRepository getIfUnique() { return null; }
            @Override public com.anz.fastpayment.router.repository.InboundMessageRepository getObject() { return null; }
            @Override public void forEach(java.util.function.Consumer action) { }
            @Override public java.util.stream.Stream stream() { return java.util.stream.Stream.empty(); }
            @Override public java.util.Iterator iterator() { return java.util.Collections.emptyIterator(); }
        };
        EventPublisher eventPublisher2 = mock(EventPublisher.class);
        UniqueIdExtractor uniqueIdExtractor2 = new UniqueIdExtractor();
        ObjectProvider<UnifiedMessageRepository> unifiedProvider2 = new ObjectProvider<>() {
            @Override public UnifiedMessageRepository getObject(Object... args) { return null; }
            @Override public UnifiedMessageRepository getIfAvailable() { return null; }
            @Override public UnifiedMessageRepository getIfUnique() { return null; }
            @Override public UnifiedMessageRepository getObject() { return null; }
            @Override public void forEach(java.util.function.Consumer action) { }
            @Override public java.util.stream.Stream stream() { return java.util.stream.Stream.empty(); }
            @Override public java.util.Iterator iterator() { return java.util.Collections.emptyIterator(); }
        };
        DuplicateChecker duplicateChecker2 = mock(DuplicateChecker.class);
        when(duplicateChecker2.isDuplicateAndRecord(anyString(), anyString(), anyString())).thenReturn(false);
        orchestrator = new RouterOrchestrator(puidGenerator, provider2, detector, xsdValidator, transformer, publisher, eventPublisher2, uniqueIdExtractor2, unifiedProvider2, duplicateChecker2, new SimpleMeterRegistry());

        orchestrator.processInboundXml(xml);

        assertTrue(capturedKey.equals("G3I0000000000002"));
        assertTrue(capturedPayload.equals(xml));
    }

    @Test
    void publishesInvalidAndPacs002RequestOnValidationFailure() {
        PuidGenerator puidGen = mock(PuidGenerator.class);
        when(puidGen.nextPuid()).thenReturn("PUID1");

        InboundMessageRepository inboundRepo = mock(InboundMessageRepository.class);
        @SuppressWarnings("unchecked")
        ObjectProvider<InboundMessageRepository> inboundProvider = (ObjectProvider<InboundMessageRepository>) mock(ObjectProvider.class);
        when(inboundProvider.getIfAvailable()).thenReturn(inboundRepo);

        Iso20022MessageTypeDetector typeDetector = mock(Iso20022MessageTypeDetector.class);
        when(typeDetector.detectType(anyString())).thenReturn("pacs.008.001.13");

        XmlSchemaValidator validator = mock(XmlSchemaValidator.class);
        doThrow(new IllegalArgumentException("XSD FAIL")).when(validator).validate(anyString(), anyString());

        Iso20022Transformer transformer = mock(Iso20022Transformer.class);
        KafkaPublisher kafkaPublisher = mock(KafkaPublisher.class);
        EventPublisher eventPublisher = mock(EventPublisher.class);
        UniqueIdExtractor uniqueIdExtractor = mock(UniqueIdExtractor.class);
        when(uniqueIdExtractor.extractUniqueId(anyString(), anyString())).thenReturn("E2E-123");

        @SuppressWarnings("unchecked")
        ObjectProvider<UnifiedMessageRepository> unifiedProvider = (ObjectProvider<UnifiedMessageRepository>) mock(ObjectProvider.class);
        when(unifiedProvider.getIfAvailable()).thenReturn(null);

        DuplicateChecker duplicateChecker3 = mock(DuplicateChecker.class);
        when(duplicateChecker3.isDuplicateAndRecord(anyString(), anyString(), anyString())).thenReturn(false);
        RouterOrchestrator orchestrator = new RouterOrchestrator(
                puidGen,
                inboundProvider,
                typeDetector,
                validator,
                transformer,
                kafkaPublisher,
                eventPublisher,
                uniqueIdExtractor,
                unifiedProvider,
                duplicateChecker3,
                new SimpleMeterRegistry()
        );

        orchestrator.processInboundXml("<xml/>");

        verify(kafkaPublisher, times(1)).publishInvalid(eq("PUID1"), anyString());
        verify(kafkaPublisher, times(1)).publishPacs002Request(eq("PUID1"), anyString());
    }
}
