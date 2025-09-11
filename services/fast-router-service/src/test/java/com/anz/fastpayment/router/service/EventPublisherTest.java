package com.anz.fastpayment.router.service;

import com.anz.fastpayment.router.model.RouterEvent;
import com.anz.fastpayment.router.repository.RouterEventRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class EventPublisherTest {

    @Test
    void publishesEventAndSavesRepository() {
        @SuppressWarnings("unchecked")
        KafkaTemplate<String, GenericRecord> kafka = mock(KafkaTemplate.class);
        when(kafka.send(any(ProducerRecord.class))).thenReturn(CompletableFuture.completedFuture(null));
        RouterEventRepository repo = mock(RouterEventRepository.class);
        EventPublisher pub = new EventPublisher(kafka, repo, new ObjectMapper());
        ReflectionTestUtils.setField(pub, "paymentEventsTopic", "payment-events");

        pub.publishPaymentReceivedEvent("P1", "G3I", "payment-messages");

        verify(kafka, atLeastOnce()).send(any(ProducerRecord.class));
        ArgumentCaptor<RouterEvent> cap = ArgumentCaptor.forClass(RouterEvent.class);
        verify(repo, atLeastOnce()).save(cap.capture());
        RouterEvent saved = cap.getValue();
        assertEquals("P1", saved.getPuid());
        assertNotNull(saved.getJson());
        assertTrue(saved.getJson().contains("\"PmtAddRq\""));
    }

    @Test
    void handlesSerializationFailureGracefully() throws Exception {
        @SuppressWarnings("unchecked")
        KafkaTemplate<String, GenericRecord> kafka = mock(KafkaTemplate.class);
        when(kafka.send(any(ProducerRecord.class))).thenReturn(CompletableFuture.completedFuture(null));
        RouterEventRepository repo = mock(RouterEventRepository.class);
        ObjectMapper om = spy(new ObjectMapper());
        doThrow(new RuntimeException("boom")).when(om).writeValueAsString(any());
        EventPublisher pub = new EventPublisher(kafka, repo, om);
        ReflectionTestUtils.setField(pub, "paymentEventsTopic", "payment-events");

        assertDoesNotThrow(() -> pub.publishPaymentReceivedEvent("P2", "G3I", "payment-messages"));
        verify(kafka, never()).send(any(ProducerRecord.class));
        verify(repo, never()).save(any());
    }
}
