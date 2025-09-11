package com.anz.fastpayment.router.service;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;

class XmlSchemaValidatorMoreEdgeTests {
	@Test
	void blankMessageType_throws() {
		XmlSchemaValidator v = new XmlSchemaValidator();
		org.springframework.test.util.ReflectionTestUtils.setField(v, "xsdValidationEnabled", true);
		assertThrows(IllegalArgumentException.class, () -> v.validate("<x/>", "  "));
	}
}
