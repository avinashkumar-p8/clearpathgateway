package com.anz.fastpayment.router.service;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class UniqueIdExtractorTest {

    @Test
    void extracts_MsgId_for_pacs008() {
        String xml = """
                <Document xmlns=\"urn:iso:std:iso:20022:tech:xsd:pacs.008.001.13\">
                  <FIToFICstmrCdtTrf><GrpHdr><MsgId>MSG-ABC</MsgId></GrpHdr></FIToFICstmrCdtTrf>
                </Document>
                """;
        String id = new UniqueIdExtractor().extractUniqueId(xml, "pacs.008.001.13");
        assertEquals("MSG-ABC", id);
    }

    @Test
    void extracts_MsgId_for_pacs003() {
        String xml = """
                <Document xmlns=\"urn:iso:std:iso:20022:tech:xsd:pacs.003.001.11\">
                  <FIToFICstmrDrctDbt><GrpHdr><MsgId>MSG-003</MsgId></GrpHdr></FIToFICstmrDrctDbt>
                </Document>
                """;
        String id = new UniqueIdExtractor().extractUniqueId(xml, "pacs.003.001.11");
        assertEquals("MSG-003", id);
    }

    @Test
    void extracts_MsgId_for_pacs007() {
        String xml = """
                <Document xmlns=\"urn:iso:std:iso:20022:tech:xsd:pacs.007.001.13\">
                  <PmtRtr><GrpHdr><MsgId>MSG-007</MsgId></GrpHdr></PmtRtr>
                </Document>
                """;
        String id = new UniqueIdExtractor().extractUniqueId(xml, "pacs.007.001.13");
        assertEquals("MSG-007", id);
    }

    @Test
    void camt056_prefers_OrgnlMsgId_then_falls_back_to_MsgId() {
        String xml = """
                <Document xmlns=\"urn:iso:std:iso:20022:tech:xsd:camt.056.001.11\">
                  <CdtrPmtActvtnReq>
                    <Undrlyg>
                      <OrgnlMsgId>OMID-056</OrgnlMsgId>
                    </Undrlyg>
                    <GrpHdr><MsgId>MSG-056</MsgId></GrpHdr>
                  </CdtrPmtActvtnReq>
                </Document>
                """;
        String id = new UniqueIdExtractor().extractUniqueId(xml, "camt.056.001.11");
        assertEquals("OMID-056", id);
    }
}





