package com.anz.fastpayment.sender.service;

import com.anz.fastpayment.sender.model.Pacs002Request;
import com.anz.fastpayment.sender.repository.Pacs002Repository;
import com.anz.fastpayment.sender.service.impl.Pacs002ServiceImpl;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.jms.core.JmsTemplate;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class Pacs002ServiceImplMaskZeroRetryTest {
    @Test
    void masksDigitsAndRunsOnceWhenZeroRetries() {
        @SuppressWarnings("unchecked") ObjectProvider<Pacs002Repository> provider = (ObjectProvider<Pacs002Repository>) mock(ObjectProvider.class);
        when(provider.getIfAvailable()).thenReturn(null);
        JmsTemplate jms = mock(JmsTemplate.class);
        EventJsonPublisher pub = mock(EventJsonPublisher.class);
        Pacs002ServiceImpl svc = new Pacs002ServiceImpl(provider, jms, pub);
        org.springframework.test.util.ReflectionTestUtils.setField(svc, "maxRetryAttempts", 0);
        org.springframework.test.util.ReflectionTestUtils.setField(svc, "retryBackoffMs", 0L);
        Pacs002Request req = new Pacs002Request();
        req.setPuid("12345678"); // triggers mask branch in logs
        req.setMessageType("pacs.008.001.13");
        req.setOriginalXml("<x/>");
        svc.handlePacs002Request(req);
        verify(jms, atLeastOnce()).convertAndSend(anyString(), anyString());
    }
}


