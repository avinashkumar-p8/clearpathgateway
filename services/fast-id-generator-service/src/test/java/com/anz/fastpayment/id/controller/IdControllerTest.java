package com.anz.fastpayment.id.controller;

import com.anz.fastpayment.id.service.IdGeneratorService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.ResponseEntity;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class IdControllerTest {

    private IdGeneratorService service;
    private IdController controller;

    @BeforeEach
    void setUp() {
        service = mock(IdGeneratorService.class);
        controller = new IdController(service);
    }

    @Test
    void puid_returns_value_from_service_and_wraps_in_map() {
        when(service.nextPuid("G31")).thenReturn("G310000000000001");
        ResponseEntity<Map<String, String>> resp = controller.puid("G31");
        assertEquals(200, resp.getStatusCode().value());
        assertEquals("G310000000000001", resp.getBody().get("puid"));
    }

    @Test
    void muid_returns_value_from_service_and_wraps_in_map() {
        when(service.nextMuid()).thenReturn("MSG0000000000001");
        ResponseEntity<Map<String, String>> resp = controller.muid();
        assertEquals(200, resp.getStatusCode().value());
        assertEquals("MSG0000000000001", resp.getBody().get("muid"));
    }

    @Test
    void puidBlock_clamps_size_and_returns_list() {
        when(service.nextPuidBlock("G31", 1)).thenReturn(List.of("G310000000000001"));
        ResponseEntity<Map<String, Object>> respSmall = controller.puidBlock("G31", 0);
        assertEquals(200, respSmall.getStatusCode().value());
        assertEquals(1, respSmall.getBody().get("count"));

        // large request clamps to 1000 but we stub for a smaller example
        when(service.nextPuidBlock("G31", 1000)).thenReturn(List.of("x"));
        ResponseEntity<Map<String, Object>> respLarge = controller.puidBlock("G31", 50000);
        assertEquals(200, respLarge.getStatusCode().value());
        assertEquals(1, respLarge.getBody().get("count"));
    }
}
