package com.anz.fastpayment.sender.controller;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class HealthControllerTest {
	@Test
	void health_ok() {
		HealthController hc = new HealthController();
		org.springframework.test.util.ReflectionTestUtils.setField(hc, "appName", "fast-sender-service");
		var resp = hc.health();
		assertEquals(200, resp.getStatusCode().value());
		Map<String, Object> body = resp.getBody();
		assertNotNull(body.get("status"));
		assertEquals("fast-sender-service", body.get("service"));
	}
}
