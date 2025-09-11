package com.anz.fastpayment.sender.messaging;

import com.anz.fastpayment.sender.model.Pacs002Request;
import com.anz.fastpayment.sender.model.Pacs002Response;
import com.anz.fastpayment.sender.service.Pacs002Service;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class Pacs002RequestConsumerDefaultsBranchTest {
    @Test
    void defaultsMessageType_whenMissing() throws Exception {
        Pacs002Service svc = mock(Pacs002Service.class);
        when(svc.handlePacs002Request(any(Pacs002Request.class))).thenReturn(new Pacs002Response("P5","ACCEPTED"));
        Pacs002RequestConsumer c = new Pacs002RequestConsumer(svc);
        String json = "{\"puid\":\"P5\",\"originalXml\":\"<x/>\"}";
        ConsumerRecord<String, byte[]> rec = new ConsumerRecord<>("pacs002-requests", 0, 0, "K", json.getBytes(java.nio.charset.StandardCharsets.UTF_8));
        c.onMessage(rec);
        verify(svc, times(1)).handlePacs002Request(any(Pacs002Request.class));
    }
}


