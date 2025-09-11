package com.anz.fastpayment.sender.service;

import com.anz.fastpayment.sender.model.Pacs002Request;
import com.anz.fastpayment.sender.repository.Pacs002Repository;
import com.anz.fastpayment.sender.service.impl.Pacs002ServiceImpl;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.jms.core.JmsTemplate;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class Pacs002ServiceImplDefaultDestinationTest {

    @Test
    void usesDefaultQueue_whenConfigBlank() {
        @SuppressWarnings("unchecked") ObjectProvider<Pacs002Repository> provider = (ObjectProvider<Pacs002Repository>) mock(ObjectProvider.class);
        when(provider.getIfAvailable()).thenReturn(null);
        JmsTemplate jms = mock(JmsTemplate.class);
        EventJsonPublisher pub = mock(EventJsonPublisher.class);
        Pacs002ServiceImpl svc = new Pacs002ServiceImpl(provider, jms, pub);
        org.springframework.test.util.ReflectionTestUtils.setField(svc, "pacs002OutboundQueue", "");
        Pacs002Request req = new Pacs002Request();
        req.setPuid("PD"); req.setMessageType("pacs.008.001.13"); req.setOriginalXml("<x/>");
        svc.handlePacs002Request(req);
        verify(jms, atLeastOnce()).convertAndSend(eq("pacs002.outbound"), anyString());
    }
}


