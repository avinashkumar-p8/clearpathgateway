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

        String payload = "{\"puid\":\"P1\",\"messageType\":\"pacs.008.001.13\",\"originalXml\":\"<x/>\",\"uniqueId\":\"E2E\"}";
        ConsumerRecord<String,String> record = new ConsumerRecord<>("pacs002-requests", 0, 0, "P1", payload);
        consumer.onMessage(record);

        verify(service, times(1)).handlePacs002Request(any(Pacs002Request.class));
    }
}
