package com.anz.fastpayment.router.service;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class XmlSchemaValidatorEdgeParamsTest {

    @Test
    void nullMessageTypeThrows() {
        XmlSchemaValidator v = new XmlSchemaValidator();
        v.setXsdValidationEnabled(true);
        assertThrows(IllegalArgumentException.class, () -> v.validate("<x/>", null));
    }

    @Test
    void emptyMessageTypeThrows() {
        XmlSchemaValidator v = new XmlSchemaValidator();
        v.setXsdValidationEnabled(true);
        assertThrows(IllegalArgumentException.class, () -> v.validate("<x/>", "   "));
    }

    @Test
    void nullXmlThrows() {
        XmlSchemaValidator v = new XmlSchemaValidator();
        v.setXsdValidationEnabled(true);
        assertThrows(IllegalArgumentException.class, () -> v.validate(null, "pacs.003.001.11"));
    }
}
