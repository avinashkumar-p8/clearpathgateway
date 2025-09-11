package com.anz.fastpayment.router.service;

import com.anz.fastpayment.router.mapping.TransformationConfigLoader;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class Iso20022TransformerMoreBranchesTest {

    private final Iso20022Transformer transformer = new Iso20022Transformer(new TransformationConfigLoader(new ObjectMapper()));

    @Test
    void pacs008_numericAmount_noCurrency_then_noAmount() throws Exception {
        String xml1 = "<Document xmlns=\"urn:iso:std:iso:20022:tech:xsd:pacs.008.001.13\">" +
                "<FIToFICstmrCdtTrf><GrpHdr><MsgId>M8</MsgId></GrpHdr>" +
                "<CdtTrfTxInf><PmtId><EndToEndId>E8</EndToEndId></PmtId><IntrBkSttlmAmt>12.34</IntrBkSttlmAmt></CdtTrfTxInf>" +
                "</FIToFICstmrCdtTrf></Document>";
        String out1 = transformer.toUnifiedJson(xml1, "pacs.008.001.13", "PU");
        assertTrue(out1.contains("\"Amount\":12.34"));
        assertFalse(out1.contains("CurCode"));

        String xml2 = "<Document xmlns=\"urn:iso:std:iso:20022:tech:xsd:pacs.008.001.13\">" +
                "<FIToFICstmrCdtTrf><GrpHdr><MsgId>M8</MsgId></GrpHdr>" +
                "<CdtTrfTxInf><PmtId><EndToEndId>E8</EndToEndId></PmtId></CdtTrfTxInf>" +
                "</FIToFICstmrCdtTrf></Document>";
        String out2 = transformer.toUnifiedJson(xml2, "pacs.008.001.13", "PU");
        assertFalse(out2.contains("Amount"));
    }

    @Test
    void pacs003_optional_missing_then_present() throws Exception {
        String xml1 = "<Document xmlns=\"urn:iso:std:iso:20022:tech:xsd:pacs.003.001.11\">" +
                "<FIToFICstmrDrctDbt><GrpHdr><MsgId>M3</MsgId></GrpHdr>" +
                "<DrctDbtTxInf><PmtId><EndToEndId>E3</EndToEndId></PmtId></DrctDbtTxInf>" +
                "</FIToFICstmrDrctDbt></Document>";
        String out1 = transformer.toUnifiedJson(xml1, "pacs.003.001.11", "PU");
        assertFalse(out1.contains("amount"));
        assertFalse(out1.contains("currency"));

        String xml2 = "<Document xmlns=\"urn:iso:std:iso:20022:tech:xsd:pacs.003.001.11\">" +
                "<FIToFICstmrDrctDbt><GrpHdr><MsgId>M3</MsgId></GrpHdr>" +
                "<DrctDbtTxInf><PmtId><EndToEndId>E3</EndToEndId></PmtId><InstdAmt Ccy=\"USD\">1.00</InstdAmt></DrctDbtTxInf>" +
                "</FIToFICstmrDrctDbt></Document>";
        String out2 = transformer.toUnifiedJson(xml2, "pacs.003.001.11", "PU");
        assertTrue(out2.contains("\"amount\":1.00"));
        assertTrue(out2.contains("\"currency\":\"USD\""));
    }

    @Test
    void pacs007_optional_missing_then_present() throws Exception {
        String xml1 = "<Document xmlns=\"urn:iso:std:iso:20022:tech:xsd:pacs.007.001.13\">" +
                "<PmtRtr><GrpHdr><MsgId>M7</MsgId></GrpHdr><TxInf/></PmtRtr></Document>";
        String out1 = transformer.toUnifiedJson(xml1, "pacs.007.001.13", "PU");
        assertFalse(out1.contains("amount"));
        assertFalse(out1.contains("currency"));

        String xml2 = "<Document xmlns=\"urn:iso:std:iso:20022:tech:xsd:pacs.007.001.13\">" +
                "<PmtRtr><GrpHdr><MsgId>M7</MsgId></GrpHdr>" +
                "<TxInf><RvslId>R1</RvslId><RvsdIntrBkSttlmAmt Ccy=\"EUR\">2.00</RvsdIntrBkSttlmAmt></TxInf>" +
                "</PmtRtr></Document>";
        String out2 = transformer.toUnifiedJson(xml2, "pacs.007.001.13", "PU");
        assertTrue(out2.contains("\"amount\":2.00"));
        assertTrue(out2.contains("\"currency\":\"EUR\""));
    }

    @Test
    void head001_missing_optional_fields() throws Exception {
        String xml = "<Document xmlns=\"urn:iso:std:iso:20022:tech:xsd:head.001.001.01\"><AppHdr/></Document>";
        String out = transformer.toUnifiedJson(xml, "head.001.001.01", "PU");
        assertTrue(out.contains("\"HEAD_001\""));
        assertFalse(out.contains("messageId"));
        assertFalse(out.contains("creationDateTime"));
    }
}
