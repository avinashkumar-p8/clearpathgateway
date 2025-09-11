package com.anz.fastpayment.router.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.util.ReflectionTestUtils;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class EventPublisherNullRepoTest {

    @Test
    void publishesWhenRepositoryIsNullViaKafkaOnly() {
        @SuppressWarnings("unchecked")
        KafkaTemplate<String, GenericRecord> tpl = (KafkaTemplate<String, GenericRecord>) mock(KafkaTemplate.class);
        when(tpl.send(any(ProducerRecord.class))).thenReturn(java.util.concurrent.CompletableFuture.completedFuture(null));
        EventPublisher ep = new EventPublisher(tpl, null, new ObjectMapper());
        ReflectionTestUtils.setField(ep, "paymentEventsTopic", "payment-events");
        assertDoesNotThrow(() -> ep.publishPaymentReceivedEvent("P2", "G3I", "payment-messages"));
        verify(tpl, times(1)).send(any(ProducerRecord.class));
    }
}
