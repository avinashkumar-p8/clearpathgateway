package com.anz.fastpayment.router.service;

import com.anz.fastpayment.router.mapping.TransformationConfigLoader;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class Iso20022TransformerAllBranchesTest {

    private final Iso20022Transformer transformer = new Iso20022Transformer(new TransformationConfigLoader(new ObjectMapper()));

    @Test
    void pacs008_optionalFieldsPresentAndNonNumericAmount() throws Exception {
        String xml = "<Document xmlns=\"urn:iso:std:iso:20022:tech:xsd:pacs.008.001.13\">" +
                "<FIToFICstmrCdtTrf><GrpHdr><MsgId>M8</MsgId><CreDtTm>2024-05-05T12:00:00Z</CreDtTm></GrpHdr>" +
                "<CdtTrfTxInf><PmtId><EndToEndId>E8</EndToEndId></PmtId><IntrBkSttlmAmt Ccy=\"USD\">N/A</IntrBkSttlmAmt></CdtTrfTxInf>" +
                "</FIToFICstmrCdtTrf></Document>";
        String out = transformer.toUnifiedJson(xml, "pacs.008.001.13", "PU");
        assertTrue(out.contains("\"CurCode\":\"USD\""));
        assertTrue(out.contains("\"Amount\":\"N/A\""));
    }

    @Test
    void pacs003_allOptionalFieldsPresent() throws Exception {
        String xml = "<Document xmlns=\"urn:iso:std:iso:20022:tech:xsd:pacs.003.001.11\">" +
                "<FIToFICstmrDrctDbt><GrpHdr><MsgId>M3</MsgId><CreDtTm>2024-02-02T00:00:00Z</CreDtTm></GrpHdr>" +
                "<DrctDbtTxInf><PmtId><EndToEndId>E3</EndToEndId></PmtId><InstdAmt Ccy=\"SGD\">10.50</InstdAmt></DrctDbtTxInf>" +
                "</FIToFICstmrDrctDbt></Document>";
        String out = transformer.toUnifiedJson(xml, "pacs.003.001.11", "PU");
        assertTrue(out.contains("\"PACS_003\""));
        assertTrue(out.contains("\"11\""));
        assertTrue(out.contains("\"messageId\":\"M3\""));
        assertTrue(out.contains("\"endToEndId\":\"E3\""));
        assertTrue(out.contains("\"amount\":10.50"));
        assertTrue(out.contains("\"currency\":\"SGD\""));
    }

    @Test
    void pacs007_allOptionalFieldsPresent() throws Exception {
        String xml = "<Document xmlns=\"urn:iso:std:iso:20022:tech:xsd:pacs.007.001.13\">" +
                "<PmtRtr><GrpHdr><MsgId>M7</MsgId><CreDtTm>2023-01-01T00:00:00Z</CreDtTm></GrpHdr>" +
                "<OrgnlGrpInf><OrgnlMsgId>ORIG7</OrgnlMsgId></OrgnlGrpInf>" +
                "<TxInf><RvslId>R1</RvslId><RvsdIntrBkSttlmAmt Ccy=\"EUR\">5.00</RvsdIntrBkSttlmAmt></TxInf>" +
                "</PmtRtr></Document>";
        String out = transformer.toUnifiedJson(xml, "pacs.007.001.13", "PU");
        assertTrue(out.contains("\"PACS_007\""));
        assertTrue(out.contains("\"originalMessageId\":\"ORIG7\""));
        assertTrue(out.contains("\"reversalId\":\"R1\""));
        assertTrue(out.contains("\"amount\":5.00"));
        assertTrue(out.contains("\"currency\":\"EUR\""));
    }

    @Test
    void camt029_minimal() throws Exception {
        String xml = "<Document xmlns=\"urn:iso:std:iso:20022:tech:xsd:camt.029.001.13\"><RsltnOfInvstgtn><Assgnmt><Id>M1</Id><CreDtTm>2024-01-02T00:00:00Z</CreDtTm></Assgnmt><Sts><AssgnmtCxlConf>true</AssgnmtCxlConf></Sts></RsltnOfInvstgtn></Document>";
        String out = transformer.toUnifiedJson(xml, "camt.029.001.13", "PU");
        assertTrue(out.contains("\"CAMT_029\""));
        assertTrue(out.contains("\"messageId\":\"M1\""));
    }

    @Test
    void head001_allOptionalFieldsPresent() throws Exception {
        String xml = "<Document xmlns=\"urn:iso:std:iso:20022:tech:xsd:head.001.001.01\">" +
                "<AppHdr><BizMsgIdr>H1</BizMsgIdr><CreDt>2024-03-03</CreDt><Fr><Id><OrgId><Id><OrgId>ZZ</OrgId></Id></OrgId></Id></Fr><To><Id><OrgId><Id><OrgId>ID</OrgId></Id></OrgId></Id></To><Sgntr><Signature>sig</Signature></Sgntr><Rltd>" +
                "<Case><Id>ID1</Id></Case></Rltd></AppHdr></Document>";
        String out = transformer.toUnifiedJson(xml, "head.001.001.01", "PU");
        assertTrue(out.contains("\"HEAD_001\""));
        assertTrue(out.contains("\"messageId\":\"H1\""));
        assertTrue(out.contains("\"messageVersion\":\"01\""));
    }
}
