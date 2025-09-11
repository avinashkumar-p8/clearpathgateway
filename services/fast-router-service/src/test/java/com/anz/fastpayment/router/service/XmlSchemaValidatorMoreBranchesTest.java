package com.anz.fastpayment.router.service;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;

class XmlSchemaValidatorMoreBranchesTest {

    @Test
    void throwsOnBlankXml() {
        XmlSchemaValidator v = new XmlSchemaValidator();
        v.setXsdValidationEnabled(true);
        assertThrows(IllegalArgumentException.class, () -> v.validate("   ", "pacs.003.001.11"));
    }

    @Test
    void throwsOnTooLargeXml() {
        XmlSchemaValidator v = new XmlSchemaValidator();
        v.setXsdValidationEnabled(true);
        v.setMaxXmlBytes(4);
        String xml = "<abcd/>";
        assertThrows(IllegalArgumentException.class, () -> v.validate(xml, "pacs.003.001.11"));
    }
}
