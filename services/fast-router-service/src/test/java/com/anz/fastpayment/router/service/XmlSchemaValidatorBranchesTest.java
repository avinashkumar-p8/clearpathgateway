package com.anz.fastpayment.router.service;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class XmlSchemaValidatorBranchesTest {

    @Test
    void returnsWhenDisabled() {
        XmlSchemaValidator v = new XmlSchemaValidator();
        org.springframework.test.util.ReflectionTestUtils.setField(v, "xsdValidationEnabled", false);
        assertDoesNotThrow(() -> v.validate("<x/>", "pacs.003.001.11"));
    }

    @Test
    void throwsOnUnsupportedType() {
        XmlSchemaValidator v = new XmlSchemaValidator();
        org.springframework.test.util.ReflectionTestUtils.setField(v, "xsdValidationEnabled", true);
        assertThrows(IllegalArgumentException.class, () -> v.validate("<x/>", "unknown"));
    }

    @Test
    void throwsOnBlankXml() {
        XmlSchemaValidator v = new XmlSchemaValidator();
        org.springframework.test.util.ReflectionTestUtils.setField(v, "xsdValidationEnabled", true);
        assertThrows(IllegalArgumentException.class, () -> v.validate("   ", "pacs.003.001.11"));
    }

    @Test
    void throwsOnOversizeXml() {
        XmlSchemaValidator v = new XmlSchemaValidator();
        org.springframework.test.util.ReflectionTestUtils.setField(v, "xsdValidationEnabled", true);
        org.springframework.test.util.ReflectionTestUtils.setField(v, "maxXmlBytes", 5);
        assertThrows(IllegalArgumentException.class, () -> v.validate("<abcdef/>", "pacs.003.001.11"));
    }
}
