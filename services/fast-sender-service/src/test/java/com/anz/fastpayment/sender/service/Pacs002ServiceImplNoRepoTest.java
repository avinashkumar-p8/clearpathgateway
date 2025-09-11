package com.anz.fastpayment.sender.service;

import com.anz.fastpayment.sender.model.Pacs002Request;
import com.anz.fastpayment.sender.service.impl.Pacs002ServiceImpl;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.jms.core.JmsTemplate;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

public class Pacs002ServiceImplNoRepoTest {

    @Test
    void worksWhenRepositoryProviderReturnsNull() {
        @SuppressWarnings("unchecked")
        ObjectProvider<com.anz.fastpayment.sender.repository.Pacs002Repository> provider = (ObjectProvider<com.anz.fastpayment.sender.repository.Pacs002Repository>) mock(ObjectProvider.class);
        when(provider.getIfAvailable()).thenReturn(null);
        JmsTemplate jms = mock(JmsTemplate.class);
        EventJsonPublisher publisher = mock(EventJsonPublisher.class);
        Pacs002ServiceImpl svc = new Pacs002ServiceImpl(provider, jms, publisher);

        Pacs002Request req = new Pacs002Request();
        req.setPuid("PNULL");
        req.setMessageType("pacs.008.001.13");
        req.setOriginalXml("<x/>");

        var resp = svc.handlePacs002Request(req);
        assertEquals("ACCEPTED", resp.getStatus());
        verify(jms, atLeastOnce()).convertAndSend(anyString(), anyString());
        verify(publisher, times(1)).publish(eq("PNULL"), anyString());
    }
}


