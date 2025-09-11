package com.anz.fastpayment.router.service;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class XmlSchemaValidatorDisabledAndSizeTest {

	@Test
	void disabled_skips_validation() {
		XmlSchemaValidator v = new XmlSchemaValidator();
		org.springframework.test.util.ReflectionTestUtils.setField(v, "xsdValidationEnabled", false);
		assertDoesNotThrow(() -> v.validate("<x/>", "pacs.008.001.13"));
	}

	@Test
	void unsupportedType_throws() {
		XmlSchemaValidator v = new XmlSchemaValidator();
		org.springframework.test.util.ReflectionTestUtils.setField(v, "xsdValidationEnabled", true);
		assertThrows(IllegalArgumentException.class, () -> v.validate("<x/>", "pacs.999.999.99"));
	}

	@Test
	void blankXml_throws() {
		XmlSchemaValidator v = new XmlSchemaValidator();
		org.springframework.test.util.ReflectionTestUtils.setField(v, "xsdValidationEnabled", true);
		assertThrows(IllegalArgumentException.class, () -> v.validate("  ", "pacs.008.001.13"));
	}

	@Test
	void oversizedXml_throws() {
		XmlSchemaValidator v = new XmlSchemaValidator();
		org.springframework.test.util.ReflectionTestUtils.setField(v, "xsdValidationEnabled", true);
		org.springframework.test.util.ReflectionTestUtils.setField(v, "maxXmlBytes", 10);
		String big = "<Document>" + "0".repeat(100) + "</Document>";
		assertThrows(IllegalArgumentException.class, () -> v.validate(big, "pacs.008.001.13"));
	}
}
