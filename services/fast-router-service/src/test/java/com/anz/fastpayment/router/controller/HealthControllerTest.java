package com.anz.fastpayment.router.controller;

import org.junit.jupiter.api.Test;
import org.springframework.http.ResponseEntity;

import java.lang.reflect.Field;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class HealthControllerTest {

    @Test
    void healthReturnsUpWithMetadata() throws Exception {
        HealthController ctrl = new HealthController();
        set(ctrl, "appName", "fast-router-service");
        set(ctrl, "appVersion", "1.0-test");

        ResponseEntity<Map<String, Object>> resp = ctrl.health();
        assertEquals(200, resp.getStatusCodeValue());
        Map<String, Object> body = resp.getBody();
        assertNotNull(body);
        assertEquals("UP", body.get("status"));
        assertEquals("fast-router-service", body.get("service"));
        assertEquals("1.0-test", body.get("version"));
        assertTrue(body.containsKey("timestamp"));
        assertTrue(body.containsKey("components"));
        Map<?,?> comps = (Map<?,?>) body.get("components");
        assertEquals("UP", comps.get("kafka"));
        assertEquals("UP", comps.get("redis"));
        assertEquals("UP", comps.get("spanner"));
    }

    private static void set(Object target, String fieldName, Object value) throws Exception {
        Field f = target.getClass().getDeclaredField(fieldName);
        f.setAccessible(true);
        f.set(target, value);
    }
}
