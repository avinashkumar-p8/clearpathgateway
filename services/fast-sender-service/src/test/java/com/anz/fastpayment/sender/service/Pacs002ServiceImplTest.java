package com.anz.fastpayment.sender.service;

import com.anz.fastpayment.sender.model.Pacs002Entity;
import com.anz.fastpayment.sender.model.Pacs002Request;
import com.anz.fastpayment.sender.repository.Pacs002Repository;
import com.anz.fastpayment.sender.service.impl.Pacs002ServiceImpl;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.jms.core.JmsTemplate;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

public class Pacs002ServiceImplTest {

    @Test
    void buildsPersistsAndPublishes() {
        Pacs002Repository repo = mock(Pacs002Repository.class);
        @SuppressWarnings("unchecked")
        ObjectProvider<Pacs002Repository> provider = (ObjectProvider<Pacs002Repository>) mock(ObjectProvider.class);
        when(provider.getIfAvailable()).thenReturn(repo);
        JmsTemplate jms = mock(JmsTemplate.class);
        EventJsonPublisher publisher = mock(EventJsonPublisher.class);
        Pacs002ServiceImpl svc = new Pacs002ServiceImpl(provider, jms, publisher);

        Pacs002Request req = new Pacs002Request();
        req.setPuid("P1");
        req.setMessageType("pacs.008.001.13");
        req.setOriginalXml("<x/>");
        req.setUniqueId("E2E");
        req.setError("XSD FAIL");

        var resp = svc.handlePacs002Request(req);
        assertEquals("P1", resp.getPuid());
        assertEquals("ACCEPTED", resp.getStatus());

        verify(repo, times(1)).save(any(Pacs002Entity.class));
        verify(jms, times(1)).convertAndSend(anyString(), anyString());
        verify(publisher, times(1)).publish(eq("P1"), anyString());
    }

    @Test
    void idempotencySkipsResendWhenExists() {
        Pacs002Repository repo = mock(Pacs002Repository.class);
        when(repo.existsById("P1")).thenReturn(true);
        @SuppressWarnings("unchecked")
        ObjectProvider<Pacs002Repository> provider = (ObjectProvider<Pacs002Repository>) mock(ObjectProvider.class);
        when(provider.getIfAvailable()).thenReturn(repo);
        JmsTemplate jms = mock(JmsTemplate.class);
        EventJsonPublisher publisher = mock(EventJsonPublisher.class);
        Pacs002ServiceImpl svc = new Pacs002ServiceImpl(provider, jms, publisher);

        Pacs002Request req = new Pacs002Request();
        req.setPuid("P1");
        req.setMessageType("pacs.008.001.13");
        req.setOriginalXml("<x/>");

        var resp = svc.handlePacs002Request(req);
        assertEquals("ACCEPTED", resp.getStatus());
        verify(jms, never()).convertAndSend(anyString(), anyString());
        verify(publisher, never()).publish(anyString(), anyString());
    }

    @Test
    void amqRetryLogsAndContinuesOnFailure() {
        Pacs002Repository repo = mock(Pacs002Repository.class);
        @SuppressWarnings("unchecked")
        ObjectProvider<Pacs002Repository> provider = (ObjectProvider<Pacs002Repository>) mock(ObjectProvider.class);
        when(provider.getIfAvailable()).thenReturn(repo);
        JmsTemplate jms = mock(JmsTemplate.class);
        doThrow(new RuntimeException("amq-down")).when(jms).convertAndSend(anyString(), anyString());
        EventJsonPublisher publisher = mock(EventJsonPublisher.class);
        Pacs002ServiceImpl svc = new Pacs002ServiceImpl(provider, jms, publisher);

        Pacs002Request req = new Pacs002Request();
        req.setPuid("P9");
        req.setMessageType("pacs.008.001.13");
        req.setOriginalXml("<x/>");

        var resp = svc.handlePacs002Request(req);
        assertEquals("ACCEPTED", resp.getStatus());
        verify(publisher, times(1)).publish(eq("P9"), anyString());
    }
}
