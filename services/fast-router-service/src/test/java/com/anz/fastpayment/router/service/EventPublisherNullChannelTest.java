package com.anz.fastpayment.router.service;

import com.anz.fastpayment.router.repository.RouterEventRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class EventPublisherNullChannelTest {
	@Test
	void channelNull_isSerializedAsEmpty() {
		@SuppressWarnings("unchecked")
		KafkaTemplate<String, GenericRecord> kafka = mock(KafkaTemplate.class);
		when(kafka.send(any(ProducerRecord.class))).thenReturn(CompletableFuture.completedFuture(null));
		RouterEventRepository repo = mock(RouterEventRepository.class);
		EventPublisher pub = new EventPublisher(kafka, repo, new ObjectMapper());
		ReflectionTestUtils.setField(pub, "paymentEventsTopic", "payment-events");
		assertDoesNotThrow(() -> pub.publishPaymentReceivedEvent("P1", null, "payment-messages"));
		verify(kafka, atLeastOnce()).send(any(ProducerRecord.class));
		verify(repo, atLeastOnce()).save(any());
	}
}
