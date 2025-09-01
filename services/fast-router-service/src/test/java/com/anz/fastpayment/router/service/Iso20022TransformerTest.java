package com.anz.fastpayment.router.service;

import com.anz.fastpayment.router.mapping.TransformationConfigLoader;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;

class Iso20022TransformerTest {

    @Test
    void transformPacs008_minimalFields_shouldContainCoreData() throws Exception {
        TransformationConfigLoader loader = new TransformationConfigLoader(new ObjectMapper());
        Iso20022Transformer transformer = new Iso20022Transformer(loader);
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
                      <Dbtr><Nm>John Doe</Nm></Dbtr>
                      <DbtrAgt><FinInstnId><BICFI>AAAAUS33</BICFI></FinInstnId></DbtrAgt>
                      <CdtrAgt><FinInstnId><BICFI>BBBBUS33</BICFI></FinInstnId></CdtrAgt>
                      <Cdtr><Nm>Jane Smith</Nm></Cdtr>
                    </CdtTrfTxInf>
                  </FIToFICstmrCdtTrf>
                </Document>
                """;

        String puid = "G3I0000000000001";
        String json = transformer.toUnifiedJson(xml, "pacs.008.001.13", puid);
        assertTrue(json.contains("\"puid\":\"" + puid + "\""));
        assertTrue(json.contains("\"messageType\":\"PACS_008\""));
        assertTrue(json.contains("\"messageVersion\":\"13\""));
        assertTrue(json.contains("\"EndToEndId\"") || json.contains("\"endToEndId\""));
        assertTrue(json.contains("\"currency\":\"SGD\""));
    }
}


