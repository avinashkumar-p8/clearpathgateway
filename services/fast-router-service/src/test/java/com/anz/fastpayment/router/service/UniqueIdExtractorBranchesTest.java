package com.anz.fastpayment.router.service;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class UniqueIdExtractorBranchesTest {

    private final UniqueIdExtractor extractor = new UniqueIdExtractor();

    @Test
    void pacsPrefersEndToEndIdThenInstrId() {
        String xml = "<Document xmlns=\"urn:iso:std:iso:20022:tech:xsd:pacs.008.001.13\">" +
                "<FIToFICstmrCdtTrf><GrpHdr><MsgId>M1</MsgId></GrpHdr>" +
                "<CdtTrfTxInf><PmtId><EndToEndId>E2E-123</EndToEndId></PmtId></CdtTrfTxInf>" +
                "</FIToFICstmrCdtTrf></Document>";
        assertEquals("E2E-123", extractor.extractUniqueId(xml, "pacs.008.001.13"));

        String xml2 = "<Document xmlns=\"urn:iso:std:iso:20022:tech:xsd:pacs.008.001.13\">" +
                "<FIToFICstmrCdtTrf><CdtTrfTxInf><PmtId><InstrId>INSTR-9</InstrId></PmtId></CdtTrfTxInf></FIToFICstmrCdtTrf></Document>";
        assertEquals("INSTR-9", extractor.extractUniqueId(xml2, "pacs.008.001.13"));
    }

    @Test
    void camt056PrefersOriginalIdsThenFallbackMsgId() {
        String xml = "<Document xmlns=\"urn:iso:std:iso:20022:tech:xsd:camt.056.001.11\">" +
                "<FIToFIPmtCxlReq><Undrlyg><OrgnlGrpInf><OrgnlMsgId>ORIG-1</OrgnlMsgId></OrgnlGrpInf></Undrlyg></FIToFIPmtCxlReq></Document>";
        assertEquals("ORIG-1", extractor.extractUniqueId(xml, "camt.056.001.11"));

        String xml2 = "<Document xmlns=\"urn:iso:std:iso:20022:tech:xsd:camt.056.001.11\">" +
                "<FIToFIPmtCxlReq><Undrlyg><OrgnlGrpInf><OrgnlInstrId>ORIG-INSTR</OrgnlInstrId></OrgnlGrpInf></Undrlyg></FIToFIPmtCxlReq></Document>";
        assertEquals("ORIG-INSTR", extractor.extractUniqueId(xml2, "camt.056.001.11"));

        String xml3 = "<Document><GrpHdr><MsgId>MSG-77</MsgId></GrpHdr></Document>";
        assertEquals("MSG-77", extractor.extractUniqueId(xml3, "camt.056.001.11"));
    }

    @Test
    void returnsEmptyWhenNothingFound() {
        String xml = "<x/>";
        assertEquals("", extractor.extractUniqueId(xml, "pacs.003.001.11"));
    }

    @Test
    void fallsBackToMsgIdWhenPacsMissingTxIds() {
        String xml = "<Document xmlns=\"urn:iso:std:iso:20022:tech:xsd:pacs.008.001.13\"><FIToFICstmrCdtTrf><GrpHdr><MsgId>MZ</MsgId></GrpHdr></FIToFICstmrCdtTrf></Document>";
        assertEquals("MZ", extractor.extractUniqueId(xml, "pacs.008.001.13"));
    }
}
