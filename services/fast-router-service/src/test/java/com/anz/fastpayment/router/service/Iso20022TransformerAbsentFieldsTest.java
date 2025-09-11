package com.anz.fastpayment.router.service;

import com.anz.fastpayment.router.mapping.TransformationConfigLoader;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class Iso20022TransformerAbsentFieldsTest {

    private final Iso20022Transformer transformer = new Iso20022Transformer(new TransformationConfigLoader(new ObjectMapper()));

    @Test
    void pacs003Minimal_noOptionalFields() throws Exception {
        String xml = "<Document xmlns=\"urn:iso:std:iso:20022:tech:xsd:pacs.003.001.11\"><FIToFICstmrDrctDbt/></Document>";
        String out = transformer.toUnifiedJson(xml, "pacs.003.001.11", "P3");
        assertTrue(out.contains("\"PACS_003\""));
        assertTrue(out.contains("\"messageVersion\":\"11\""));
    }

    @Test
    void pacs007Minimal_noOptionalFields() throws Exception {
        String xml = "<Document xmlns=\"urn:iso:std:iso:20022:tech:xsd:pacs.007.001.13\"><PmtRtr/></Document>";
        String out = transformer.toUnifiedJson(xml, "pacs.007.001.13", "P7");
        assertTrue(out.contains("\"PACS_007\""));
        assertTrue(out.contains("\"messageVersion\":\"13\""));
    }

    @Test
    void camt056Minimal_noOptionalFields() throws Exception {
        String xml = "<Document xmlns=\"urn:iso:std:iso:20022:tech:xsd:camt.056.001.11\"><FIToFIPmtCxlReq/></Document>";
        String out = transformer.toUnifiedJson(xml, "camt.056.001.11", "C56");
        assertTrue(out.contains("\"CAMT_056\""));
        assertTrue(out.contains("\"messageVersion\":\"11\""));
    }

    @Test
    void head001Minimal_noOptionalFields() throws Exception {
        String xml = "<Document xmlns=\"urn:iso:std:iso:20022:tech:xsd:head.001.001.01\"><AppHdr/></Document>";
        String out = transformer.toUnifiedJson(xml, "head.001.001.01", "H1");
        assertTrue(out.contains("\"HEAD_001\""));
        assertTrue(out.contains("\"messageVersion\":\"01\""));
    }
}
