package com.anz.fastpayment.sender.service;

import com.anz.fastpayment.sender.model.Camt029Request;
import com.anz.fastpayment.sender.service.impl.Camt029ServiceImpl;
import org.junit.jupiter.api.Test;
import org.springframework.jms.core.JmsTemplate;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class Camt029ServiceImplMoreTest {

    @Test
    void includesOriginalInstrId_whenPresent_inCamt056() {
        JmsTemplate j = mock(JmsTemplate.class);
        Camt029ServiceImpl svc = new Camt029ServiceImpl(j);
        org.springframework.test.util.ReflectionTestUtils.setField(svc, "camt029OutboundQueue", "q");
        Camt029Request req = new Camt029Request();
        req.setPuid("PX"); req.setMessageType("camt.056.001.11");
        req.setOriginalXml("<Document xmlns=\"urn:iso:std:iso:20022:tech:xsd:camt.056.001.11\"><FIToFIPmtCxlReq><OrgnlInstrId>INSTR-1</OrgnlInstrId></FIToFIPmtCxlReq></Document>");
        String xml = svc.handleCamt029Request(req);
        assertTrue(xml.contains("OrgnlInstrId"));
        assertTrue(xml.contains("INSTR-1"));
        verify(j, atLeastOnce()).convertAndSend(anyString(), anyString());
    }

    @Test
    void decision_isCANC_whenNoError_elseRJCT() {
        JmsTemplate j = mock(JmsTemplate.class);
        Camt029ServiceImpl svc = new Camt029ServiceImpl(j);
        org.springframework.test.util.ReflectionTestUtils.setField(svc, "camt029OutboundQueue", "q");

        Camt029Request ok = new Camt029Request();
        ok.setPuid("P1"); ok.setMessageType("camt.056.001.11"); ok.setOriginalXml("<x/>");
        String xmlOk = svc.handleCamt029Request(ok);
        assertTrue(xmlOk.contains("<AssgnmtCxlConf>true</AssgnmtCxlConf>"));
        assertTrue(xmlOk.contains("<Prtry>CANC</Prtry>"));

        Camt029Request rej = new Camt029Request();
        rej.setPuid("P2"); rej.setMessageType("camt.056.001.11"); rej.setOriginalXml("<x/>"); rej.setError("reason");
        String xmlRej = svc.handleCamt029Request(rej);
        assertTrue(xmlRej.contains("<AssgnmtCxlConf>false</AssgnmtCxlConf>"));
        assertTrue(xmlRej.contains("<Prtry>RJCT</Prtry>"));
    }

    @Test
    void defaultDestination_whenBlankQueueConfig() {
        JmsTemplate j = mock(JmsTemplate.class);
        Camt029ServiceImpl svc = new Camt029ServiceImpl(j);
        org.springframework.test.util.ReflectionTestUtils.setField(svc, "camt029OutboundQueue", "");
        Camt029Request req = new Camt029Request();
        req.setPuid("PA"); req.setMessageType("camt.056.001.11"); req.setOriginalXml("<x/>");
        svc.handleCamt029Request(req);
        verify(j, atLeastOnce()).convertAndSend(eq("camt29.outbound"), anyString());
    }
}


