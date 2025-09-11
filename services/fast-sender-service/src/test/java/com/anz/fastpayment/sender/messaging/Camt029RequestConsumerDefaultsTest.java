package com.anz.fastpayment.sender.messaging;

import com.anz.fastpayment.sender.model.Camt029Request;
import com.anz.fastpayment.sender.service.Camt029Service;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;

import static org.mockito.Mockito.*;

class Camt029RequestConsumerDefaultsTest {

    @Test
    void defaultsMessageType_whenMissing() {
        Camt029Service svc = mock(Camt029Service.class);
        when(svc.handleCamt029Request(any())).thenReturn("x");
        Camt029RequestConsumer c = new Camt029RequestConsumer(svc);
        String json = "{\"puid\":\"C4\",\"payload\":\"<x/>\"}";
        ConsumerRecord<String, byte[]> rec = new ConsumerRecord<>("camt029-requests", 0, 0, "K", json.getBytes(java.nio.charset.StandardCharsets.UTF_8));
        c.onMessage(rec);
        verify(svc, times(1)).handleCamt029Request(any(Camt029Request.class));
    }

    @Test
    void nullRecord_isIgnored() {
        Camt029Service svc = mock(Camt029Service.class);
        Camt029RequestConsumer c = new Camt029RequestConsumer(svc);
        c.onMessage(null);
        verify(svc, never()).handleCamt029Request(any());
    }
}


