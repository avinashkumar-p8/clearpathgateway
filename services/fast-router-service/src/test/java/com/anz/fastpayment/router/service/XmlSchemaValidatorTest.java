package com.anz.fastpayment.router.service;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class XmlSchemaValidatorTest {

    @Test
    void disabledSkipsValidation() {
        XmlSchemaValidator v = new XmlSchemaValidator();
        v.setXsdValidationEnabled(false);
        v.validate("<x/>", "pacs.003.001.11");
    }

    @Test
    void rejectsOversizedXml() {
        XmlSchemaValidator v = new XmlSchemaValidator();
        v.setXsdValidationEnabled(true);
        v.setMaxXmlBytes(2);
        assertThrows(IllegalArgumentException.class, () -> v.validate("<xxxx>", "pacs.003.001.11"));
    }

    @Test
    void rejectsUnknownType() {
        XmlSchemaValidator v = new XmlSchemaValidator();
        v.setXsdValidationEnabled(true);
        assertThrows(IllegalArgumentException.class, () -> v.validate("<x/>", "unknown"));
    }
}


