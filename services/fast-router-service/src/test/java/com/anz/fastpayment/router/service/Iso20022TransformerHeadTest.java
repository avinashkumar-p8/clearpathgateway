package com.anz.fastpayment.router.service;

import com.anz.fastpayment.router.mapping.TransformationConfigLoader;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class Iso20022TransformerHeadTest {

    @Test
    void transformsHead001Minimal() throws Exception {
        Iso20022Transformer t = new Iso20022Transformer(new TransformationConfigLoader(new com.fasterxml.jackson.databind.ObjectMapper()));
        String xml = "<?xml version=\"1.0\"?><AppHdr xmlns=\"urn:iso:std:iso:20022:tech:xsd:head.001.001.01\"><Fr><FIId><FinInstnId><BICFI>AAAABBCCDDD</BICFI></FinInstnId></FIId></Fr></AppHdr>";
        String json = t.toUnifiedJson(xml, "head.001.001.01", "P1");
        assertTrue(json.contains("\"messageType\":\"HEAD_001\""));
        assertTrue(json.contains("\"puid\":\"P1\""));
    }

    @Test
    void unknownTypeReturnsMinimal() throws Exception {
        Iso20022Transformer t = new Iso20022Transformer(new TransformationConfigLoader(new com.fasterxml.jackson.databind.ObjectMapper()));
        String xml = "<x/>";
        String json = t.toUnifiedJson(xml, "unknown", "P2");
        assertTrue(json.contains("\"puid\":\"P2\""));
    }
}
