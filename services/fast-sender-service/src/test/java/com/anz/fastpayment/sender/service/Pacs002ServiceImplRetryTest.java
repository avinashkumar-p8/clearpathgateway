package com.anz.fastpayment.sender.service;

import com.anz.fastpayment.sender.model.Pacs002Request;
import com.anz.fastpayment.sender.repository.Pacs002Repository;
import com.anz.fastpayment.sender.service.impl.Pacs002ServiceImpl;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.jms.core.JmsTemplate;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class Pacs002ServiceImplRetryTest {
	@Test
	void retries_multiple_times_on_failure() {
		Pacs002Repository repo = mock(Pacs002Repository.class);
		@SuppressWarnings("unchecked") ObjectProvider<Pacs002Repository> provider = (ObjectProvider<Pacs002Repository>) mock(ObjectProvider.class);
		when(provider.getIfAvailable()).thenReturn(repo);
		JmsTemplate jms = mock(JmsTemplate.class);
		doThrow(new RuntimeException("amq-down")).when(jms).convertAndSend(anyString(), anyString());
		EventJsonPublisher pub = mock(EventJsonPublisher.class);
		Pacs002ServiceImpl svc = new Pacs002ServiceImpl(provider, jms, pub);
		org.springframework.test.util.ReflectionTestUtils.setField(svc, "maxRetryAttempts", 3);
		org.springframework.test.util.ReflectionTestUtils.setField(svc, "retryBackoffMs", 1L);
		Pacs002Request req = new Pacs002Request();
		req.setPuid("PR"); req.setMessageType("pacs.008.001.13"); req.setOriginalXml("<x/>");
		svc.handlePacs002Request(req);
		verify(jms, atLeast(3)).convertAndSend(anyString(), anyString());
	}
}
