package com.anz.fastpayment.router.service;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class UniqueIdExtractorTest {

    @Test
    void pacsPrefersEndToEndIdOverInstrId() {
        String xml = "" +
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
                "<Document xmlns=\"urn:iso:std:iso:20022:tech:xsd:pacs.008.001.13\">" +
                "  <FIToFICstmrCdtTrf>" +
                "    <CdtTrfTxInf>" +
                "      <PmtId>" +
                "        <InstrId>INSTR-1</InstrId>" +
                "        <EndToEndId>E2E-1</EndToEndId>" +
                "      </PmtId>" +
                "    </CdtTrfTxInf>" +
                "  </FIToFICstmrCdtTrf>" +
                "</Document>";
        UniqueIdExtractor ext = new UniqueIdExtractor();
        String id = ext.extractUniqueId(xml, "pacs.008.001.13");
        assertEquals("E2E-1", id);
    }

    @Test
    void camtPrefersOrgnlMsgIdThenOrgnlInstrId() {
        String xml = "" +
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
                "<Document xmlns=\"urn:iso:std:iso:20022:tech:xsd:camt.056.001.11\">" +
                "  <FIToFIPmtCxlReq>" +
                "    <OrgnlGrpInf>" +
                "      <OrgnlMsgId>OMID-1</OrgnlMsgId>" +
                "    </OrgnlGrpInf>" +
                "    <OrgnlTxInf>" +
                "      <OrgnlInstrId>OINS-1</OrgnlInstrId>" +
                "    </OrgnlTxInf>" +
                "  </FIToFIPmtCxlReq>" +
                "</Document>";
        UniqueIdExtractor ext = new UniqueIdExtractor();
        String id = ext.extractUniqueId(xml, "camt.056.001.11");
        assertEquals("OMID-1", id);
    }

    @Test
    void extracts_MsgId_for_pacs008() {
        String xml = """
                <Document xmlns=\"urn:iso:std:iso:20022:tech:xsd:pacs.008.001.13\">\n  <FIToFICstmrCdtTrf><GrpHdr><MsgId>MSG-ABC</MsgId></GrpHdr></FIToFICstmrCdtTrf>\n</Document>
                """;
        String id = new UniqueIdExtractor().extractUniqueId(xml, "pacs.008.001.13");
        assertEquals("MSG-ABC", id);
    }

    @Test
    void extracts_MsgId_for_pacs003() {
        String xml = """
                <Document xmlns=\"urn:iso:std:iso:20022:tech:xsd:pacs.003.001.11\">\n  <FIToFICstmrDrctDbt><GrpHdr><MsgId>MSG-003</MsgId></GrpHdr></FIToFICstmrDrctDbt>\n</Document>
                """;
        String id = new UniqueIdExtractor().extractUniqueId(xml, "pacs.003.001.11");
        assertEquals("MSG-003", id);
    }

    @Test
    void extracts_MsgId_for_pacs007() {
        String xml = """
                <Document xmlns=\"urn:iso:std:iso:20022:tech:xsd:pacs.007.001.13\">\n  <PmtRtr><GrpHdr><MsgId>MSG-007</MsgId></GrpHdr></PmtRtr>\n</Document>
                """;
        String id = new UniqueIdExtractor().extractUniqueId(xml, "pacs.007.001.13");
        assertEquals("MSG-007", id);
    }

    @Test
    void camt056_prefers_OrgnlMsgId_then_falls_back_to_MsgId() {
        String xml = """
                <Document xmlns=\"urn:iso:std:iso:20022:tech:xsd:camt.056.001.11\">\n  <CdtrPmtActvtnReq>\n    <Undrlyg>\n      <OrgnlMsgId>OMID-056</OrgnlMsgId>\n    </Undrlyg>\n    <GrpHdr><MsgId>MSG-056</MsgId></GrpHdr>\n  </CdtrPmtActvtnReq>\n</Document>
                """;
        String id = new UniqueIdExtractor().extractUniqueId(xml, "camt.056.001.11");
        assertEquals("OMID-056", id);
    }
}





