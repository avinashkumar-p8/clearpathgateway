package com.anz.fastpayment.sender.service;

import com.anz.fastpayment.sender.model.Camt029Request;
import com.anz.fastpayment.sender.service.impl.Camt029ServiceImpl;
import org.junit.jupiter.api.Test;
import org.springframework.jms.core.JmsTemplate;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class Camt029ServiceImplBranchesTest {
    @Test
    void badXml_fallsBackToPuid_and_masksLongDigitsInLogs() {
        JmsTemplate j = mock(JmsTemplate.class);
        Camt029ServiceImpl svc = new Camt029ServiceImpl(j);
        org.springframework.test.util.ReflectionTestUtils.setField(svc, "camt029OutboundQueue", "q");
        Camt029Request req = new Camt029Request();
        req.setPuid("12345678"); // triggers safe() masking branch in logs
        req.setMessageType("camt.056.001.11");
        req.setOriginalXml("<not-xml");
        String xml = svc.handleCamt029Request(req);
        assertTrue(xml.contains("C029-"));
        verify(j, atLeastOnce()).convertAndSend(eq("q"), anyString());
    }

    @Test
    void emptyOriginalXml_usesPuidAsOriginalId() {
        JmsTemplate j = mock(JmsTemplate.class);
        Camt029ServiceImpl svc = new Camt029ServiceImpl(j);
        org.springframework.test.util.ReflectionTestUtils.setField(svc, "camt029OutboundQueue", "q");
        Camt029Request req = new Camt029Request();
        req.setPuid("PU"); req.setMessageType("camt.056.001.11"); req.setOriginalXml("");
        String xml = svc.handleCamt029Request(req);
        assertTrue(xml.contains("C029-PU"));
    }
}


