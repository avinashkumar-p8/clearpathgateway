package com.anz.fastpayment.router.service;

import com.anz.fastpayment.router.mapping.TransformationConfigLoader;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class Iso20022TransformerFieldsTest {

    private final Iso20022Transformer transformer = new Iso20022Transformer(new TransformationConfigLoader(new ObjectMapper()));

    @Test
    void pacs008NumericAndNonNumericAmounts() throws Exception {
        String xmlNum = "<Document xmlns=\"urn:iso:std:iso:20022:tech:xsd:pacs.008.001.13\">" +
                "<FIToFICstmrCdtTrf><GrpHdr><MsgId>M1</MsgId><CreDtTm>2024-01-01T00:00:00Z</CreDtTm></GrpHdr>" +
                "<CdtTrfTxInf><PmtId><EndToEndId>E2E-1</EndToEndId></PmtId>" +
                "<IntrBkSttlmAmt Ccy=\"SGD\">123.45</IntrBkSttlmAmt></CdtTrfTxInf></FIToFICstmrCdtTrf></Document>";
        String json1 = transformer.toUnifiedJson(xmlNum, "pacs.008.001.13", "P1");
        assertTrue(json1.contains("\"Amount\":123.45"));
        assertTrue(json1.contains("\"CurCode\":\"SGD\""));

        String xmlStrAmt = xmlNum.replace("123.45", "ABC");
        String json2 = transformer.toUnifiedJson(xmlStrAmt, "pacs.008.001.13", "P1");
        assertTrue(json2.contains("\"Amount\":\"ABC\""));
    }

    @Test
    void pacs003OptionalFieldsOmitted() throws Exception {
        String xml = "<Document xmlns=\"urn:iso:std:iso:20022:tech:xsd:pacs.003.001.11\">" +
                "<FIToFICstmrDrctDbt><GrpHdr></GrpHdr><DrctDbtTxInf></DrctDbtTxInf></FIToFICstmrDrctDbt></Document>";
        String json = transformer.toUnifiedJson(xml, "pacs.003.001.11", "P2");
        assertTrue(json.contains("\"messageType\":\"PACS_003\""));
        assertTrue(json.contains("\"transactions\":[{"));
        assertFalse(json.contains("endToEndId"));
        assertTrue(json.endsWith("}\n"));
    }
}
