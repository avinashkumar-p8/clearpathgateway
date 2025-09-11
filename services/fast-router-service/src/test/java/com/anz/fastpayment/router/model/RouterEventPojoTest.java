package com.anz.fastpayment.router.model;

import org.junit.jupiter.api.Test;
import java.time.Instant;
import static org.junit.jupiter.api.Assertions.*;

class RouterEventPojoTest {
    @Test
    void gettersSetters() {
        RouterEvent e = new RouterEvent();
        e.setPuid("P1");
        e.setTopic("t");
        Instant now = Instant.now();
        e.setCreatedAt(now);
        e.setJson("{}");
        assertEquals("P1", e.getPuid());
        assertEquals("t", e.getTopic());
        assertEquals(now, e.getCreatedAt());
        assertEquals("{}", e.getJson());
    }
}
