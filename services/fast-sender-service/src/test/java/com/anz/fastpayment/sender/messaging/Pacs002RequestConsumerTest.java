package com.anz.fastpayment.sender.messaging;

import com.anz.fastpayment.sender.model.Pacs002Request;
import com.anz.fastpayment.sender.model.Pacs002Response;
import com.anz.fastpayment.sender.service.Pacs002Service;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;

import static org.mockito.Mockito.*;

public class Pacs002RequestConsumerTest {

    @Test
    void consumesAndDelegates() throws Exception {
        Pacs002Service service = mock(Pacs002Service.class);
        when(service.handlePacs002Request(any())).thenReturn(new Pacs002Response("P1","ACCEPTED"));
        Pacs002RequestConsumer consumer = new Pacs002RequestConsumer(service);

        String json = "{\"puid\":\"P1\",\"messageType\":\"pacs.008.001.13\",\"originalXml\":\"<x/>\",\"uniqueId\":\"E2E\"}";
        byte[] jsonBytes = json.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        byte[] wire = new byte[1 + 4 + jsonBytes.length];
        wire[0] = 0; // magic
        // schema id 1
        wire[1] = 0; wire[2] = 0; wire[3] = 0; wire[4] = 1;
        System.arraycopy(jsonBytes, 0, wire, 5, jsonBytes.length);
        ConsumerRecord<String,byte[]> record = new ConsumerRecord<>("pacs002-requests", 0, 0, "P1", wire);
        consumer.onMessage(record);

        verify(service, times(1)).handlePacs002Request(any(Pacs002Request.class));
    }

    @Test
    void consumesPlainJsonFallback() {
        Pacs002Service service = mock(Pacs002Service.class);
        when(service.handlePacs002Request(any())).thenReturn(new Pacs002Response("P2","ACCEPTED"));
        Pacs002RequestConsumer consumer = new Pacs002RequestConsumer(service);

        String json = "{\"puid\":\"P2\",\"originalXml\":\"<x/>\"}";
        ConsumerRecord<String,byte[]> record = new ConsumerRecord<>("pacs002-requests", 0, 0, "P2", json.getBytes(java.nio.charset.StandardCharsets.UTF_8));
        consumer.onMessage(record);
        verify(service, times(1)).handlePacs002Request(any(Pacs002Request.class));
    }

    @Test
    void consumesAvroMagicButDecodeFailureFallsBackToJsonSlice() {
        Pacs002Service service = mock(Pacs002Service.class);
        when(service.handlePacs002Request(any())).thenReturn(new Pacs002Response("P3","ACCEPTED"));
        Pacs002RequestConsumer consumer = new Pacs002RequestConsumer(service);

        // magic + id + garbage json bytes that are still parseable
        byte[] payload = "{\"puid\":\"P3\",\"originalXml\":\"<y/>\"}".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        byte[] wire = new byte[1 + 4 + payload.length];
        wire[0] = 0; wire[1]=0; wire[2]=0; wire[3]=0; wire[4]=1;
        System.arraycopy(payload, 0, wire, 5, payload.length);
        ConsumerRecord<String,byte[]> record = new ConsumerRecord<>("pacs002-requests", 0, 0, "P3", wire);
        consumer.onMessage(record);
        verify(service, times(1)).handlePacs002Request(any(Pacs002Request.class));
    }

    @Test
    void ignoresEmptyPayload() {
        Pacs002Service service = mock(Pacs002Service.class);
        Pacs002RequestConsumer consumer = new Pacs002RequestConsumer(service);
        ConsumerRecord<String,byte[]> record = new ConsumerRecord<>("pacs002-requests", 0, 0, "K", new byte[0]);
        consumer.onMessage(record);
        verify(service, never()).handlePacs002Request(any());
    }
}
