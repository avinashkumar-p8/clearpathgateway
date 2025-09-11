package com.anz.fastpayment.router.service;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;

class XmlSchemaValidatorRealValidateTest {

    private XmlSchemaValidator v;

    @BeforeEach
    void setUp() {
        v = new XmlSchemaValidator();
        org.springframework.test.util.ReflectionTestUtils.setField(v, "xsdValidationEnabled", true);
        org.springframework.test.util.ReflectionTestUtils.setField(v, "maxXmlBytes", 10_000_000);
        org.springframework.test.util.ReflectionTestUtils.setField(v, "testBypassValidation", false);
    }

    @Test
    void pacs003_real_minimal_invalid_throws() {
        String xml = """
                <Document xmlns="urn:iso:std:iso:20022:tech:xsd:pacs.003.001.11">
                  <FIToFICstmrDrctDbt>
                    <GrpHdr><MsgId>M3</MsgId><CreDtTm>2024-01-01T00:00:00</CreDtTm><NbOfTxs>1</NbOfTxs><SttlmInf><SttlmMtd>CLRG</SttlmMtd></SttlmInf></GrpHdr>
                    <DrctDbtTxInf>
                      <PmtId><EndToEndId>E3</EndToEndId></PmtId>
                      <IntrBkSttlmAmt Ccy="SGD">1.00</IntrBkSttlmAmt>
                      <ChrgBr>DEBT</ChrgBr>
                      <Dbtr><Nm>Y</Nm></Dbtr>
                    </DrctDbtTxInf>
                  </FIToFICstmrDrctDbt>
                </Document>
                """;
        assertThrows(IllegalArgumentException.class, () -> v.validate(xml, "pacs.003.001.11"));
    }

    @Test
    void head001_real_minimal_invalid_throws() {
        String xml = """
                <AppHdr xmlns="urn:iso:std:iso:20022:tech:xsd:head.001.001.01">
                  <Fr><OrgId>FR</OrgId></Fr>
                  <To><OrgId>TO</OrgId></To>
                  <BizMsgIdr>ID</BizMsgIdr>
                  <MsgDefIdr>pacs.003.001.11</MsgDefIdr>
                  <CreDt>2024-01-01T00:00:00Z</CreDt>
                </AppHdr>
                """;
        assertThrows(IllegalArgumentException.class, () -> v.validate(xml, "head.001.001.01"));
    }
}
