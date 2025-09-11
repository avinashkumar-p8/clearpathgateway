package com.anz.fastpayment.sender.service;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.concurrent.CompletableFuture;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

class KafkaPublisherTest {

	@Test
	void nonAvroPayload_isSkipped() {
		KafkaTemplate<String, GenericRecord> kt = mock(KafkaTemplate.class);
		KafkaPublisher p = new KafkaPublisher(kt);
		p.publish("t", "not-avro");
		verifyNoInteractions(kt);
	}

	@Test
	void success_callback_logs() {
		KafkaTemplate<String, GenericRecord> kt = mock(KafkaTemplate.class);
		CompletableFuture<org.springframework.kafka.support.SendResult<String, GenericRecord>> fut = new CompletableFuture<>();
		org.apache.kafka.clients.producer.RecordMetadata md = new org.apache.kafka.clients.producer.RecordMetadata(new org.apache.kafka.common.TopicPartition("t", 0), 0, 1, 0L, Long.valueOf(0), 0, 0);
		org.springframework.kafka.support.SendResult<String, GenericRecord> sr = new org.springframework.kafka.support.SendResult<>(new org.apache.kafka.clients.producer.ProducerRecord<>("t", null), md);
		fut.complete(sr);
		when(kt.send(eq("t"), any(GenericRecord.class))).thenReturn(fut);
		KafkaPublisher p = new KafkaPublisher(kt);
		Schema s = new Schema.Parser().parse("{\n \"type\":\"record\",\n \"name\":\"E\",\n \"fields\":[{\"name\":\"a\",\"type\":\"string\"}]\n}");
		GenericRecord rec = new GenericData.Record(s);
		rec.put("a", "b");
		p.publish("t", rec);
		verify(kt, times(1)).send(eq("t"), any(GenericRecord.class));
	}

	@Test
	void failure_callback_logs() {
		KafkaTemplate<String, GenericRecord> kt = mock(KafkaTemplate.class);
		CompletableFuture<org.springframework.kafka.support.SendResult<String, GenericRecord>> fut = new CompletableFuture<>();
		fut.completeExceptionally(new RuntimeException("x"));
		when(kt.send(eq("t"), any(GenericRecord.class))).thenReturn(fut);
		KafkaPublisher p = new KafkaPublisher(kt);
		Schema s = new Schema.Parser().parse("{\n \"type\":\"record\",\n \"name\":\"E\",\n \"fields\":[{\"name\":\"a\",\"type\":\"string\"}]\n}");
		GenericRecord rec = new GenericData.Record(s);
		rec.put("a", "b");
		p.publish("t", rec);
		verify(kt, times(1)).send(eq("t"), any(GenericRecord.class));
	}
}
