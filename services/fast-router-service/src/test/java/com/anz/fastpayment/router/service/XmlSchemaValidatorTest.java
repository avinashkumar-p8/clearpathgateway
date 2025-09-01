package com.anz.fastpayment.router.service;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

class XmlSchemaValidatorTest {

    private XmlSchemaValidator validator;

    @BeforeEach
    void setUp() {
        validator = new XmlSchemaValidator();
    }

    @Test
    void validPacs008_shouldPass() {
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

        assertDoesNotThrow(() -> validator.validate(xml, "pacs.008.001.13"));
    }

    @Test
    void invalidPacs008_missingDbtrAgt_shouldFail() {
        String xml = """
                <Document xmlns=\"urn:iso:std:iso:20022:tech:xsd:pacs.008.001.13\">
                  <FIToFICstmrCdtTrf>
                    <GrpHdr>
                      <MsgId>MSG-001</MsgId>
                      <CreDtTm>2024-01-15T10:30:00Z</CreDtTm>
                      <NbOfTxs>1</NbOfTxs>
                      <SttlmInf><SttlmMtd>CLRG</SttlmMtd></SttlmInf>
                    </GrpHdr>
                    <CdtTrfTxInf>
                      <PmtId><EndToEndId>E2E-001</EndToEndId></PmtId>
                      <IntrBkSttlmAmt Ccy=\"SGD\">1000.00</IntrBkSttlmAmt>
                      <ChrgBr>SHAR</ChrgBr>
                      <Cdtr><Nm>Jane Smith</Nm></Cdtr>
                    </CdtTrfTxInf>
                  </FIToFICstmrCdtTrf>
                </Document>
                """;

        assertThrows(IllegalArgumentException.class, () -> validator.validate(xml, "pacs.008.001.13"));
    }
}


