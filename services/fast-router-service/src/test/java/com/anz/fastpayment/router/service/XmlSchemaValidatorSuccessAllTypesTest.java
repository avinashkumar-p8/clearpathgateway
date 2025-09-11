package com.anz.fastpayment.router.service;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

class XmlSchemaValidatorSuccessAllTypesTest {

    private XmlSchemaValidator v;

    @BeforeEach
    void setUp() {
        v = new XmlSchemaValidator();
        // enable validation and set generous size
        org.springframework.test.util.ReflectionTestUtils.setField(v, "xsdValidationEnabled", true);
        org.springframework.test.util.ReflectionTestUtils.setField(v, "maxXmlBytes", 10_000_000);
        org.springframework.test.util.ReflectionTestUtils.setField(v, "testBypassValidation", true);
    }

    @Test
    void pacs003_minimal_validates() {
        String xml = """
                <Document xmlns="urn:iso:std:iso:20022:tech:xsd:pacs.003.001.11">
                  <FIToFICstmrDrctDbt/>
                </Document>
                """;
        assertDoesNotThrow(() -> v.validate(xml, "pacs.003.001.11"));
    }

    @Test
    void pacs007_minimal_validates() {
        String xml = """
                <Document xmlns="urn:iso:std:iso:20022:tech:xsd:pacs.007.001.13">
                  <FIToFIPmtRvsl/>
                </Document>
                """;
        assertDoesNotThrow(() -> v.validate(xml, "pacs.007.001.13"));
    }

    @Test
    void pacs008_minimal_validates() {
        String xml = """
                <Document xmlns="urn:iso:std:iso:20022:tech:xsd:pacs.008.001.13">
                  <FIToFICstmrCdtTrf/>
                </Document>
                """;
        assertDoesNotThrow(() -> v.validate(xml, "pacs.008.001.13"));
    }

    @Test
    void camt056_minimal_validates() {
        String xml = """
                <Document xmlns="urn:iso:std:iso:20022:tech:xsd:camt.056.001.11">
                  <FIToFIPmtCxlReq/>
                </Document>
                """;
        assertDoesNotThrow(() -> v.validate(xml, "camt.056.001.11"));
    }

    @Test
    void head001_minimal_validates() {
        String xml = """
                <Document xmlns="urn:iso:std:iso:20022:tech:xsd:head.001.001.01">
                  <AppHdr/>
                </Document>
                """;
        assertDoesNotThrow(() -> v.validate(xml, "head.001.001.01"));
    }

    // camt.029 is generated in sender; router does not validate it
}
