package com.anz.fastpayment.sender.service;

import com.anz.fastpayment.sender.model.Pacs002Entity;
import com.anz.fastpayment.sender.model.Pacs002Request;
import com.anz.fastpayment.sender.repository.Pacs002Repository;
import com.anz.fastpayment.sender.service.impl.Pacs002ServiceImpl;
import org.junit.jupiter.api.Test;
import org.springframework.jms.core.JmsTemplate;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

public class Pacs002ServiceImplTest {

    @Test
    void buildsPersistsAndPublishes() {
        Pacs002Repository repo = mock(Pacs002Repository.class);
        JmsTemplate jms = mock(JmsTemplate.class);
        EventJsonPublisher publisher = mock(EventJsonPublisher.class);
        Pacs002ServiceImpl svc = new Pacs002ServiceImpl(repo, jms, publisher);

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
}
