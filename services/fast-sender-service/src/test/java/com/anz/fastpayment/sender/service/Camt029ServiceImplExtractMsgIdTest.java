package com.anz.fastpayment.sender.service;

import com.anz.fastpayment.sender.model.Camt029Request;
import com.anz.fastpayment.sender.service.impl.Camt029ServiceImpl;
import org.junit.jupiter.api.Test;
import org.springframework.jms.core.JmsTemplate;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class Camt029ServiceImplExtractMsgIdTest {
    @Test
    void extractsOrgnlMsgId_whenPresent() {
        JmsTemplate j = mock(JmsTemplate.class);
        Camt029ServiceImpl svc = new Camt029ServiceImpl(j);
        org.springframework.test.util.ReflectionTestUtils.setField(svc, "camt029OutboundQueue", "q");
        Camt029Request req = new Camt029Request();
        req.setPuid("PC"); req.setMessageType("camt.056.001.11");
        req.setOriginalXml("<Document xmlns=\"urn:iso:std:iso:20022:tech:xsd:camt.056.001.11\"><FIToFIPmtCxlReq><OrgnlMsgId>MID-1</OrgnlMsgId></FIToFIPmtCxlReq></Document>");
        String xml = svc.handleCamt029Request(req);
        assertTrue(xml.contains("MID-1"));
        verify(j, atLeastOnce()).convertAndSend(anyString(), anyString());
    }
}


