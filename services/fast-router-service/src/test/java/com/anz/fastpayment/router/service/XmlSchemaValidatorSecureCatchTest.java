package com.anz.fastpayment.router.service;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

class XmlSchemaValidatorSecureCatchTest {

	@Test
	void securePropertiesCatch_then_bypass_success() {
		XmlSchemaValidator v = new XmlSchemaValidator();
		org.springframework.test.util.ReflectionTestUtils.setField(v, "xsdValidationEnabled", true);
		org.springframework.test.util.ReflectionTestUtils.setField(v, "maxXmlBytes", 10_000_000);
		org.springframework.test.util.ReflectionTestUtils.setField(v, "testForceSecurePropertiesCatch", true);
		org.springframework.test.util.ReflectionTestUtils.setField(v, "testBypassValidation", true);
		assertDoesNotThrow(() -> v.validate("<Document xmlns=\"urn:iso:std:iso:20022:tech:xsd:head.001.001.01\"><AppHdr><BizMsgIdr>X</BizMsgIdr><CreDt>2024-01-01</CreDt><Fr><OrgId/></Fr><To><OrgId/></To></AppHdr></Document>", "head.001.001.01"));
	}
}
