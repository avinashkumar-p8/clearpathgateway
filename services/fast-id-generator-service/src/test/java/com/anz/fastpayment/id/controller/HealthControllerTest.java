package com.anz.fastpayment.id.controller;

import org.junit.jupiter.api.Test;
import org.springframework.http.ResponseEntity;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class HealthControllerTest {

    @Test
    void health_returns_up_status() {
        HealthController hc = new HealthController();
        ResponseEntity<Map<String, Object>> resp = hc.health();
        assertEquals(200, resp.getStatusCode().value());
        assertEquals("UP", resp.getBody().get("status"));
        assertTrue(resp.getBody().containsKey("timestamp"));
    }
}
