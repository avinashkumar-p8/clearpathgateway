package com.anz.fastpayment.sender.messaging;

import com.anz.fastpayment.sender.model.Pacs002Request;
import com.anz.fastpayment.sender.model.Pacs002Response;
import com.anz.fastpayment.sender.service.Pacs002Service;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class Pacs002RequestConsumerMoreTest {

	private Pacs002RequestConsumer build(Pacs002Service svc) {
		return new Pacs002RequestConsumer(svc);
	}

	@Test
	void emptyPayload_isIgnored() {
		Pacs002Service svc = mock(Pacs002Service.class);
		Pacs002RequestConsumer c = build(svc);
		ConsumerRecord<String, byte[]> rec = new ConsumerRecord<>("pacs002-requests", 0, 1L, "K", new byte[0]);
		c.onMessage(rec);
		verifyNoInteractions(svc);
	}

	@Test
	void plainJson_isProcessed() throws Exception {
		Pacs002Service svc = mock(Pacs002Service.class);
		when(svc.handlePacs002Request(any(Pacs002Request.class))).thenReturn(new Pacs002Response("P1","ACCEPTED"));
		Pacs002RequestConsumer c = build(svc);
		String json = "{\"puid\":\"P1\",\"messageType\":\"pacs.008.001.13\",\"originalXml\":\"<x/>\"}";
		ConsumerRecord<String, byte[]> rec = new ConsumerRecord<>("pacs002-requests", 0, 2L, "K", json.getBytes());
		c.onMessage(rec);
		verify(svc, times(1)).handlePacs002Request(any(Pacs002Request.class));
	}

	@Test
	void avroFramed_withFallbackJson_isProcessed() throws Exception {
		Pacs002Service svc = mock(Pacs002Service.class);
		when(svc.handlePacs002Request(any(Pacs002Request.class))).thenReturn(new Pacs002Response("P2","ACCEPTED"));
		Pacs002RequestConsumer c = build(svc);
		byte[] payload = "{\"puid\":\"P2\",\"payload\":\"<x/>\"}".getBytes();
		byte[] framed = new byte[payload.length + 5];
		framed[0]=0; // magic
		System.arraycopy(payload, 0, framed, 5, payload.length);
		ConsumerRecord<String, byte[]> rec = new ConsumerRecord<>("pacs002-requests", 0, 3L, "K", framed);
		c.onMessage(rec);
		verify(svc, times(1)).handlePacs002Request(any(Pacs002Request.class));
	}

	@Test
	void missingPuid_isIgnored() throws Exception {
		Pacs002Service svc = mock(Pacs002Service.class);
		Pacs002RequestConsumer c = build(svc);
		String json = "{\"messageType\":\"pacs.002.request\",\"originalXml\":\"<x/>\"}";
		ConsumerRecord<String, byte[]> rec = new ConsumerRecord<>("pacs002-requests", 0, 4L, "K", json.getBytes());
		c.onMessage(rec);
		verifyNoInteractions(svc);
	}

	@Test
	void parseError_isCaught() {
		Pacs002Service svc = mock(Pacs002Service.class);
		Pacs002RequestConsumer c = build(svc);
		ConsumerRecord<String, byte[]> rec = new ConsumerRecord<>("pacs002-requests", 0, 5L, "K", "{not-json".getBytes());
		c.onMessage(rec);
		verifyNoInteractions(svc);
	}
}
