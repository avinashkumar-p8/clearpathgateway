package com.anz.fastpayment.router.service;

import com.anz.fastpayment.router.mapping.TransformationConfigLoader;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class Iso20022TransformerMoreFieldsTest {

    private final Iso20022Transformer transformer = new Iso20022Transformer(new TransformationConfigLoader(new ObjectMapper()));

    @Test
    void pacs007WithReversalFields() throws Exception {
        String xml = "<Document xmlns=\"urn:iso:std:iso:20022:tech:xsd:pacs.007.001.13\">" +
                "<PmtRtr><GrpHdr><MsgId>M1</MsgId><CreDtTm>2024-01-01T00:00:00Z</CreDtTm></GrpHdr>" +
                "<OrgnlGrpInf><OrgnlMsgId>OM1</OrgnlMsgId></OrgnlGrpInf>" +
                "<TxInf><OrgnlTxRef><RvslId>RV1</RvslId><RvsdIntrBkSttlmAmt Ccy=\"USD\">10.00</RvsdIntrBkSttlmAmt></OrgnlTxRef></TxInf>" +
                "</PmtRtr></Document>";
        String json = transformer.toUnifiedJson(xml, "pacs.007.001.13", "PZ");
        assertTrue(json.contains("\"messageType\":\"PACS_007\""));
        assertTrue(json.contains("\"reversalId\":\"RV1\""));
        assertTrue(json.contains("\"amount\":10.00"));
        assertTrue(json.contains("\"currency\":\"USD\""));
    }

    @Test
    void camt056WithIds() throws Exception {
        String xml = "<Document xmlns=\"urn:iso:std:iso:20022:tech:xsd:camt.056.001.11\">" +
                "<FIToFIPmtCxlReq><Case><Id>C1</Id></Case><Undrlyg><OrgnlGrpInf><OrgnlMsgId>OM2</OrgnlMsgId></OrgnlGrpInf></Undrlyg><GrpHdr><CreDtTm>2024-01-01T00:00:00Z</CreDtTm></GrpHdr></FIToFIPmtCxlReq></Document>";
        String json = transformer.toUnifiedJson(xml, "camt.056.001.11", "PC");
        assertTrue(json.contains("\"caseId\":\"C1\""));
        assertTrue(json.contains("\"originalMessageId\":\"OM2\""));
        assertTrue(json.contains("\"creationDateTime\":"));
    }

    @Test
    void head001WithFields() throws Exception {
        String xml = "<Document xmlns=\"urn:iso:std:iso:20022:tech:xsd:head.001.001.01\">" +
                "<AppHdr><BizMsgIdr>BZ1</BizMsgIdr><CreDt>2024-01-01</CreDt></AppHdr><Pyld><Any><Id>H1</Id></Any></Pyld></Document>";
        String json = transformer.toUnifiedJson(xml, "head.001.001.01", "PH");
        assertTrue(json.contains("\"messageType\":\"HEAD_001\""));
        assertTrue(json.contains("\"messageId\":\"BZ1\""));
        assertTrue(json.contains("\"creationDateTime\":\"2024-01-01\""));
        assertTrue(json.contains("\"headerId\":\"H1\""));
    }
}
