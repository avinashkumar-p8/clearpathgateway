package com.anz.fastpayment.sender.service;

import com.anz.fastpayment.sender.model.Camt029Request;
import com.anz.fastpayment.sender.service.impl.Camt029ServiceImpl;
import org.junit.jupiter.api.Test;
import org.springframework.jms.core.JmsTemplate;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class Camt029ServiceImplTest {

    @Test
    void buildsAndSendsCamt029() {
        JmsTemplate j = mock(JmsTemplate.class);
        Camt029ServiceImpl svc = new Camt029ServiceImpl(j);
        org.springframework.test.util.ReflectionTestUtils.setField(svc, "camt029OutboundQueue", "q");
        Camt029Request req = new Camt029Request();
        req.setPuid("P"); req.setMessageType("camt.056.001.11"); req.setOriginalXml("<x/>");
        String xml = svc.handleCamt029Request(req);
        assertNotNull(xml);
        verify(j, atLeastOnce()).convertAndSend(anyString(), anyString());
    }

    @Test
    void retriesAndLogsOnFailure_thenGivesUp() {
        JmsTemplate j = mock(JmsTemplate.class);
        doThrow(new RuntimeException("amq-down")).when(j).convertAndSend(anyString(), anyString());
        Camt029ServiceImpl svc = new Camt029ServiceImpl(j);
        org.springframework.test.util.ReflectionTestUtils.setField(svc, "camt029OutboundQueue", "q");
        org.springframework.test.util.ReflectionTestUtils.setField(svc, "maxRetryAttempts", 2);
        org.springframework.test.util.ReflectionTestUtils.setField(svc, "retryBackoffMs", 0L);
        Camt029Request req = new Camt029Request();
        req.setPuid("PR"); req.setMessageType("camt.056.001.11"); req.setOriginalXml("<x/>");
        String xml = svc.handleCamt029Request(req);
        assertTrue(xml.contains("camt.029"));
        verify(j, times(2)).convertAndSend(anyString(), anyString());
    }
}


