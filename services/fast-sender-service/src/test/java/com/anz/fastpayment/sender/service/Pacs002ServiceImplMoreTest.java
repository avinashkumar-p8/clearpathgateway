package com.anz.fastpayment.sender.service;

import com.anz.fastpayment.sender.model.Pacs002Request;
import com.anz.fastpayment.sender.repository.Pacs002Repository;
import com.anz.fastpayment.sender.service.impl.Pacs002ServiceImpl;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.jms.core.JmsTemplate;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class Pacs002ServiceImplMoreTest {

	private Pacs002ServiceImpl build(Pacs002Repository repo, JmsTemplate jms, EventJsonPublisher pub) {
		@SuppressWarnings("unchecked") ObjectProvider<Pacs002Repository> provider = (ObjectProvider<Pacs002Repository>) mock(ObjectProvider.class);
		when(provider.getIfAvailable()).thenReturn(repo);
		return new Pacs002ServiceImpl(provider, jms, pub);
	}

	@Test
	void idempotencyCheckException_isCaught() {
		Pacs002Repository repo = mock(Pacs002Repository.class);
		when(repo.existsById(anyString())).thenThrow(new RuntimeException("repo-down"));
		JmsTemplate jms = mock(JmsTemplate.class);
		EventJsonPublisher pub = mock(EventJsonPublisher.class);
		Pacs002ServiceImpl svc = build(repo, jms, pub);
		Pacs002Request req = new Pacs002Request();
		req.setPuid("PZ"); req.setMessageType("pacs.008.001.13"); req.setOriginalXml("<x/>");
		svc.handlePacs002Request(req);
		verify(jms, atLeastOnce()).convertAndSend(anyString(), anyString());
	}

	@Test
	void persistSaveException_isCaught() {
		Pacs002Repository repo = mock(Pacs002Repository.class);
		when(repo.save(any())).thenThrow(new RuntimeException("save-fail"));
		JmsTemplate jms = mock(JmsTemplate.class);
		EventJsonPublisher pub = mock(EventJsonPublisher.class);
		Pacs002ServiceImpl svc = build(repo, jms, pub);
		Pacs002Request req = new Pacs002Request();
		req.setPuid("PY"); req.setMessageType("pacs.008.001.13"); req.setOriginalXml("<x/>");
		svc.handlePacs002Request(req);
		verify(jms, atLeastOnce()).convertAndSend(anyString(), anyString());
	}

	@Test
	void eventPublishException_isCaught() {
		Pacs002Repository repo = mock(Pacs002Repository.class);
		JmsTemplate jms = mock(JmsTemplate.class);
		EventJsonPublisher pub = mock(EventJsonPublisher.class);
		doThrow(new RuntimeException("send-fail")).when(pub).publish(anyString(), anyString());
		Pacs002ServiceImpl svc = build(repo, jms, pub);
		Pacs002Request req = new Pacs002Request();
		req.setPuid("PK"); req.setMessageType("pacs.008.001.13"); req.setOriginalXml("<x/>");
		svc.handlePacs002Request(req);
		verify(pub, times(1)).publish(eq("PK"), anyString());
	}

	@Test
	void extractOrMsgId_success_and_fallback() {
		Pacs002Repository repo = mock(Pacs002Repository.class);
		JmsTemplate jms = mock(JmsTemplate.class);
		EventJsonPublisher pub = mock(EventJsonPublisher.class);
		Pacs002ServiceImpl svc = build(repo, jms, pub);
		Pacs002Request ok = new Pacs002Request();
		ok.setPuid("PA"); ok.setMessageType("pacs.008.001.13");
		ok.setOriginalXml("<Document xmlns=\"urn:iso:std:iso:20022:tech:xsd:pacs.002.001.15\"><FIToFIPmtStsRpt><OrgnlGrpInfAndSts><OrgnlMsgId>ORIG-1</OrgnlMsgId></OrgnlGrpInfAndSts></FIToFIPmtStsRpt></Document>");
		svc.handlePacs002Request(ok);
		verify(jms, atLeastOnce()).convertAndSend(anyString(), contains("ORIG-1"));

		Pacs002Request bad = new Pacs002Request();
		bad.setPuid("PB"); bad.setMessageType("pacs.008.001.13");
		bad.setOriginalXml("<not-xml");
		svc.handlePacs002Request(bad);
		verify(jms, atLeastOnce()).convertAndSend(anyString(), contains("P002-PB"));
	}
}
