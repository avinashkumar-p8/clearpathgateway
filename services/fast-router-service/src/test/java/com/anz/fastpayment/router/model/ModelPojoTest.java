package com.anz.fastpayment.router.model;

import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.*;

class ModelPojoTest {

    @Test
    void inboundMessagePojo() {
        InboundMessage m = new InboundMessage();
        m.setPuid("P1");
        m.setChannelId("G3I");
        m.setMessageType("pacs.003.001.11");
        Instant now = Instant.now();
        m.setReceivedAt(now);
        m.setRawXml("<x/>");
        m.setStatus("OK");
        m.setError("E");
        assertEquals("P1", m.getPuid());
        assertEquals("G3I", m.getChannelId());
        assertEquals("pacs.003.001.11", m.getMessageType());
        assertEquals(now, m.getReceivedAt());
        assertEquals("<x/>", m.getRawXml());
        assertEquals("OK", m.getStatus());
        assertEquals("E", m.getError());
    }

    @Test
    void unifiedMessagePojo() {
        UnifiedMessage u = new UnifiedMessage();
        u.setPuid("P2");
        u.setMessageType("pacs.008.001.13");
        Instant now = Instant.now();
        u.setCreatedAt(now);
        u.setJson("{}\n");
        assertEquals("P2", u.getPuid());
        assertEquals("pacs.008.001.13", u.getMessageType());
        assertEquals(now, u.getCreatedAt());
        assertEquals("{}\n", u.getJson());
    }
}
