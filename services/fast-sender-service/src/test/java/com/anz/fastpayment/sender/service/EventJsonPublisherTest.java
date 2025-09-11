package com.anz.fastpayment.sender.service;

import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaTemplate;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

public class EventJsonPublisherTest {

    @Test
    void publishesAvroPaymentEvent() {
        @SuppressWarnings("unchecked")
        KafkaTemplate<String, GenericRecord> template = (KafkaTemplate<String, GenericRecord>) mock(KafkaTemplate.class);
        when(template.send(any(), anyString(), any())).thenReturn(java.util.concurrent.CompletableFuture.completedFuture(null));

        EventJsonPublisher pub = new EventJsonPublisher(template);
        pub.publish("K1", "{\"ok\":true}");

        verify(template, times(1)).send(any(), eq("K1"), argThat(v -> v instanceof GenericRecord));
    }

    @Test
    void publish_handles_send_exception_gracefully() {
        @SuppressWarnings("unchecked")
        KafkaTemplate<String, GenericRecord> template = (KafkaTemplate<String, GenericRecord>) mock(KafkaTemplate.class);
        when(template.send(any(), anyString(), any())).thenThrow(new RuntimeException("send-fail"));

        EventJsonPublisher pub = new EventJsonPublisher(template);
        // Should not throw
        pub.publish("K2", "{\"ok\":false}");
        verify(template, times(1)).send(any(), eq("K2"), any());
    }
}


