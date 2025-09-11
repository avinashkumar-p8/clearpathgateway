package com.anz.fastpayment.sender.service;

import com.anz.fastpayment.sender.Pacs002Publisher;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import jakarta.jms.JMSException;
import org.junit.jupiter.api.Test;
import org.springframework.jms.JmsException;
import org.springframework.jms.core.JmsTemplate;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

class Pacs002PublisherMoreTest {

	@Test
	void nullPayload_isIgnored() {
		JmsTemplate jt = mock(JmsTemplate.class);
		Pacs002Publisher p = new Pacs002Publisher(jt, "pacs002.outbound", new SimpleMeterRegistry());
		p.sendPacs002Xml(null);
		verifyNoInteractions(jt);
	}

	@Test
	void success_incrementsCounter() {
		JmsTemplate jt = mock(JmsTemplate.class);
		Pacs002Publisher p = new Pacs002Publisher(jt, "pacs002.outbound", new SimpleMeterRegistry());
		p.sendPacs002Xml("<x/>");
		verify(jt, times(1)).convertAndSend(anyString(), anyString(), any());
	}

	@Test
	void failure_isCaught_andCounterIncremented() {
		JmsTemplate jt = mock(JmsTemplate.class);
		doThrow(new JmsException("down"){}).when(jt).convertAndSend(anyString(), anyString(), any());
		Pacs002Publisher p = new Pacs002Publisher(jt, "pacs002.outbound", new SimpleMeterRegistry());
		p.sendPacs002Xml("<x/>");
		verify(jt, times(1)).convertAndSend(anyString(), anyString(), any());
	}
}
