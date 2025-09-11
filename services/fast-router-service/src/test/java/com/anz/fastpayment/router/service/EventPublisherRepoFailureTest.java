package com.anz.fastpayment.router.service;

import com.anz.fastpayment.router.repository.RouterEventRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.util.ReflectionTestUtils;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

class EventPublisherRepoFailureTest {
    @Test
    void repoFailureIsSwallowed() {
        @SuppressWarnings("unchecked")
        KafkaTemplate<String, GenericRecord> avro = mock(KafkaTemplate.class);
        when(avro.send(any(ProducerRecord.class))).thenReturn(java.util.concurrent.CompletableFuture.completedFuture(null));
        RouterEventRepository repo = mock(RouterEventRepository.class);
        doThrow(new RuntimeException("db fail")).when(repo).save(any());
        EventPublisher pub = new EventPublisher(avro, repo, new ObjectMapper());
        ReflectionTestUtils.setField(pub, "paymentEventsTopic", "payment-events");
        assertDoesNotThrow(() -> pub.publishPaymentReceivedEvent("PZ", "G3I", "payment-messages"));
        verify(avro).send(any(ProducerRecord.class));
        verify(repo).save(any());
    }
}
