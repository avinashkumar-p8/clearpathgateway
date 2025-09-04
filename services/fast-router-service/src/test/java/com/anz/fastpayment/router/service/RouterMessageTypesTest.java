package com.anz.fastpayment.router.service;

import com.anz.fastpayment.router.repository.InboundMessageRepository;
import com.anz.fastpayment.router.repository.UnifiedMessageRepository;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.avro.Schema;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.ObjectProvider;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class RouterMessageTypesTest {

    private KafkaPublisher publisher;
    private XmlSchemaValidator validator;
    private Iso20022Transformer transformer;
    private Iso20022MessageTypeDetector detector;
    private UniqueIdExtractor uniqueIdExtractor;

    private ObjectProvider<InboundMessageRepository> inboundProvider;
    private ObjectProvider<UnifiedMessageRepository> unifiedProvider;

    @BeforeEach
    void setup() {
        publisher = mock(KafkaPublisher.class);
        // universal schema for Avro record building in tests
        Schema s = new Schema.Parser().parse("{\n  \"type\": \"record\",\n  \"name\": \"UnifiedPaymentMessage\",\n  \"fields\": [\n    { \"name\": \"messageType\", \"type\": \"string\" },\n    { \"name\": \"messageVersion\", \"type\": [\"null\", \"string\"], \"default\": null },\n    { \"name\": \"messageId\", \"type\": \"string\" },\n    { \"name\": \"creationDateTime\", \"type\": \"string\" },\n    { \"name\": \"supplementaryData\", \"type\": [\"null\", {\"type\": \"map\", \"values\": \"string\"}], \"default\": null }\n  ]\n}");
        when(publisher.getUnifiedSchema()).thenReturn(s);
        validator = new XmlSchemaValidator();
        try {
            var f = XmlSchemaValidator.class.getDeclaredField("xsdValidationEnabled"); f.setAccessible(true); f.set(validator, true);
            var m = XmlSchemaValidator.class.getDeclaredField("maxXmlBytes"); m.setAccessible(true); m.setInt(validator, 10_000_000);
        } catch (Exception ignore) {}
        transformer = new Iso20022Transformer(new com.anz.fastpayment.router.mapping.TransformationConfigLoader(new com.fasterxml.jackson.databind.ObjectMapper()));
        detector = new Iso20022MessageTypeDetector();
        uniqueIdExtractor = new UniqueIdExtractor();
        inboundProvider = new EmptyProvider<>();
        unifiedProvider = new EmptyProvider<>();
    }

    @Test
    void pacs003_valid_flows_to_avro() {
        RouterOrchestrator orch = buildOrchestrator("G3I0000000000003", false);
        String xml = """
                <Document xmlns=\"urn:iso:std:iso:20022:tech:xsd:pacs.003.001.11\">
                  <FIToFICstmrDrctDbt></FIToFICstmrDrctDbt>
                </Document>
                """;
        orch.processInboundXml(xml);
        ArgumentCaptor<org.apache.avro.generic.GenericRecord> rec = ArgumentCaptor.forClass(org.apache.avro.generic.GenericRecord.class);
        verify(publisher, times(1)).publishValidUnified(rec.capture(), eq("G3I0000000000003"));
        assertEquals("PACS_003", String.valueOf(rec.getValue().get("messageType")));
        assertEquals("11", rec.getValue().get("messageVersion"));
    }

    @Test
    void pacs007_valid_flows_to_avro() {
        RouterOrchestrator orch = buildOrchestrator("G3I0000000000004", false);
        String xml = """
                <Document xmlns=\"urn:iso:std:iso:20022:tech:xsd:pacs.007.001.13\">
                  <FIToFIPmtRvsl></FIToFIPmtRvsl>
                </Document>
                """;
        orch.processInboundXml(xml);
        ArgumentCaptor<org.apache.avro.generic.GenericRecord> rec = ArgumentCaptor.forClass(org.apache.avro.generic.GenericRecord.class);
        verify(publisher, times(1)).publishValidUnified(rec.capture(), eq("G3I0000000000004"));
        assertEquals("PACS_007", String.valueOf(rec.getValue().get("messageType")));
        assertEquals("13", rec.getValue().get("messageVersion"));
    }

    @Test
    void camt056_valid_flows_to_avro() {
        RouterOrchestrator orch = buildOrchestrator("G3I0000000000005", false);
        String xml = """
                <Document xmlns=\"urn:iso:std:iso:20022:tech:xsd:camt.056.001.11\">
                  <FIToFIPmtCxlReq>
                    <Assgnmt></Assgnmt>
                  </FIToFIPmtCxlReq>
                </Document>
                """;
        orch.processInboundXml(xml);
        ArgumentCaptor<org.apache.avro.generic.GenericRecord> rec = ArgumentCaptor.forClass(org.apache.avro.generic.GenericRecord.class);
        verify(publisher, times(1)).publishValidUnified(rec.capture(), eq("G3I0000000000005"));
        assertEquals("CAMT_056", String.valueOf(rec.getValue().get("messageType")));
        assertEquals("11", rec.getValue().get("messageVersion"));
    }

    @Test
    void duplicate_flow_skips_publish_for_pacs008_by_InstrId() {
        DuplicateChecker dup = mock(DuplicateChecker.class);
        when(dup.isDuplicateAndRecord(anyString(), anyString(), anyString())).thenReturn(true);
        RouterOrchestrator orch = buildOrchestrator("G3I0000000000006", true, dup);
        String xml = """
                <Document xmlns=\"urn:iso:std:iso:20022:tech:xsd:pacs.008.001.13\">
                  <FIToFICstmrCdtTrf>
                    <GrpHdr><MsgId>M</MsgId><CreDtTm>2024-01-01T00:00:00Z</CreDtTm><NbOfTxs>1</NbOfTxs><SttlmInf><SttlmMtd>CLRG</SttlmMtd></SttlmInf></GrpHdr>
                    <CdtTrfTxInf>
                      <PmtId><InstrId>INSTR-DUP-1</InstrId></PmtId>
                      <IntrBkSttlmAmt Ccy=\"SGD\">1.00</IntrBkSttlmAmt>
                    </CdtTrfTxInf>
                  </FIToFICstmrCdtTrf>
                </Document>
                """;
        orch.processInboundXml(xml);
        verify(publisher, never()).publishValidUnified(any(), any());
    }

    @Test
    void duplicate_flow_skips_publish_for_camt056_by_OrgnlInstrId() {
        DuplicateChecker dup = mock(DuplicateChecker.class);
        when(dup.isDuplicateAndRecord(anyString(), anyString(), anyString())).thenReturn(true);
        RouterOrchestrator orch = buildOrchestrator("G3I0000000000007", true, dup);
        String xml = """
                <Document xmlns=\"urn:iso:std:iso:20022:tech:xsd:camt.056.001.11\">
                  <FIToFIPmtCxlReq>
                    <OrgnlGrpInf><OrgnlMsgId>MSG-1</OrgnlMsgId></OrgnlGrpInf>
                    <OrgnlInstrId>INSTR-DUP-2</OrgnlInstrId>
                  </FIToFIPmtCxlReq>
                </Document>
                """;
        orch.processInboundXml(xml);
        verify(publisher, never()).publishValidUnified(any(), any());
    }

    private RouterOrchestrator buildOrchestrator(String puid, boolean enableXsd) {
        return buildOrchestrator(puid, enableXsd, mock(DuplicateChecker.class));
    }

    private RouterOrchestrator buildOrchestrator(String puid, boolean enableXsd, DuplicateChecker duplicateChecker) {
        PuidGenerator pg = new PuidGenerator() {
            @Override
            public String nextPuid() {
                return puid;
            }
        };
        EventPublisher eventPublisher = mock(EventPublisher.class);
        try {
            var f = XmlSchemaValidator.class.getDeclaredField("xsdValidationEnabled");
            f.setAccessible(true);
            f.set(validator, enableXsd);
            var m = XmlSchemaValidator.class.getDeclaredField("maxXmlBytes");
            m.setAccessible(true);
            m.setInt(validator, 10_000_000);
        } catch (Exception ignore) {}
        when(duplicateChecker.isDuplicateAndRecord(anyString(), anyString(), anyString())).thenReturn(false);
        return new RouterOrchestrator(
                pg,
                inboundProvider,
                detector,
                validator,
                transformer,
                publisher,
                eventPublisher,
                uniqueIdExtractor,
                unifiedProvider,
                duplicateChecker,
                new com.fasterxml.jackson.databind.ObjectMapper(),
                new SimpleMeterRegistry()
        );
    }

    static class EmptyProvider<T> implements ObjectProvider<T> {
        @Override public T getObject(Object... args) { return null; }
        @Override public T getIfAvailable() { return null; }
        @Override public T getIfUnique() { return null; }
        @Override public T getObject() { return null; }
        @Override public void forEach(java.util.function.Consumer action) { }
        @Override public java.util.stream.Stream<T> stream() { return java.util.stream.Stream.empty(); }
        @Override public java.util.Iterator<T> iterator() { return java.util.Collections.emptyIterator(); }
    }
}


