package com.anz.fastpayment.sender.service;

import com.anz.fastpayment.sender.model.Camt029Request;
import com.anz.fastpayment.sender.service.impl.Camt029ServiceImpl;
import org.junit.jupiter.api.Test;
import org.springframework.jms.core.JmsTemplate;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class Camt029ServiceImplDefaultQueueNullTest {
    @Test
    void nullQueue_usesDefault() {
        JmsTemplate j = mock(JmsTemplate.class);
        Camt029ServiceImpl svc = new Camt029ServiceImpl(j);
        org.springframework.test.util.ReflectionTestUtils.setField(svc, "camt029OutboundQueue", null);
        Camt029Request req = new Camt029Request();
        req.setPuid("PZ"); req.setMessageType("camt.056.001.11"); req.setOriginalXml("<x/>");
        svc.handleCamt029Request(req);
        verify(j, atLeastOnce()).convertAndSend(eq("camt29.outbound"), anyString());
    }
}


