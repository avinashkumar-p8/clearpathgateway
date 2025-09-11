package com.anz.fastpayment.sender.messaging;

import com.anz.fastpayment.sender.model.WrapperRequest;
import com.anz.fastpayment.sender.service.EventPublisher;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

class WrapperRequestConsumerMoreTest {

	private WrapperRequestConsumer build(EventPublisher pub) {
		return new WrapperRequestConsumer(pub, new ObjectMapper());
	}

	@Test
	void nullPayload_isIgnored() {
		EventPublisher pub = mock(EventPublisher.class);
		WrapperRequestConsumer c = build(pub);
		ConsumerRecord<String, String> rec = new ConsumerRecord<>("sender-wrapper-requests", 0, 1L, "K", null);
		c.onMessage(rec);
		verifyNoInteractions(pub);
	}

	@Test
	void invalidJson_isIgnored() {
		EventPublisher pub = mock(EventPublisher.class);
		WrapperRequestConsumer c = build(pub);
		ConsumerRecord<String, String> rec = new ConsumerRecord<>("sender-wrapper-requests", 0, 1L, "K", "{not-json");
		c.onMessage(rec);
		verifyNoInteractions(pub);
	}

	@Test
	void missingTrailerServiceStatus_isIgnored() throws Exception {
		EventPublisher pub = mock(EventPublisher.class);
		WrapperRequestConsumer c = build(pub);
		WrapperRequest w = new WrapperRequest();
		w.setTrailer(new WrapperRequest.Trailer());
		String payload = new ObjectMapper().writeValueAsString(w);
		ConsumerRecord<String, String> rec = new ConsumerRecord<>("sender-wrapper-requests", 0, 2L, "K", payload);
		c.onMessage(rec);
		verifyNoInteractions(pub);
	}

	@Test
	void happyPath_publishesEvent() throws Exception {
		EventPublisher pub = mock(EventPublisher.class);
		WrapperRequestConsumer c = build(pub);
		WrapperRequest.ServiceStatus ss = new WrapperRequest.ServiceStatus();
		ss.setStatusCode("0000");
		ss.setStatusDesc("OK");
		WrapperRequest.Trailer tr = new WrapperRequest.Trailer();
		tr.setServiceStatus(ss);
		WrapperRequest w = new WrapperRequest();
		w.setTrailer(tr);
		String payload = new ObjectMapper().writeValueAsString(w);
		ConsumerRecord<String, String> rec = new ConsumerRecord<>("sender-wrapper-requests", 0, 3L, "K", payload);
		c.onMessage(rec);
		verify(pub, atLeastOnce()).publish(any(), any());
	}
}
