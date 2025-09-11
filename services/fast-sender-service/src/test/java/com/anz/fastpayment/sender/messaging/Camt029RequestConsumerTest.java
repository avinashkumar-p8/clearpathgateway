package com.anz.fastpayment.sender.messaging;

import com.anz.fastpayment.sender.model.Camt029Request;
import com.anz.fastpayment.sender.service.Camt029Service;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;

import static org.mockito.Mockito.*;

class Camt029RequestConsumerTest {

    @Test
    void consumesPlainJsonAndDelegates() {
        Camt029Service svc = mock(Camt029Service.class);
        when(svc.handleCamt029Request(any())).thenReturn("x");
        Camt029RequestConsumer c = new Camt029RequestConsumer(svc);
        String json = "{\"puid\":\"C1\",\"messageType\":\"camt.056.001.11\",\"originalXml\":\"<x/>\"}";
        ConsumerRecord<String, byte[]> rec = new ConsumerRecord<>("camt029-requests", 0, 0, "K", json.getBytes(java.nio.charset.StandardCharsets.UTF_8));
        c.onMessage(rec);
        verify(svc, times(1)).handleCamt029Request(any(Camt029Request.class));
    }

    @Test
    void ignoresMissingPuid() {
        Camt029Service svc = mock(Camt029Service.class);
        Camt029RequestConsumer c = new Camt029RequestConsumer(svc);
        String json = "{\"originalXml\":\"<x/>\"}";
        ConsumerRecord<String, byte[]> rec = new ConsumerRecord<>("camt029-requests", 0, 0, "K", json.getBytes(java.nio.charset.StandardCharsets.UTF_8));
        c.onMessage(rec);
        verify(svc, never()).handleCamt029Request(any());
    }

    @Test
    void avroMagicSlicesToJson() {
        Camt029Service svc = mock(Camt029Service.class);
        when(svc.handleCamt029Request(any())).thenReturn("x");
        Camt029RequestConsumer c = new Camt029RequestConsumer(svc);
        byte[] payload = "{\"puid\":\"C2\",\"originalXml\":\"<y/>\"}".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        byte[] wire = new byte[1 + 4 + payload.length];
        wire[0]=0; wire[1]=0; wire[2]=0; wire[3]=0; wire[4]=1; System.arraycopy(payload, 0, wire, 5, payload.length);
        ConsumerRecord<String, byte[]> rec = new ConsumerRecord<>("camt029-requests", 0, 0, "K", wire);
        c.onMessage(rec);
        verify(svc, times(1)).handleCamt029Request(any(Camt029Request.class));
    }
}


