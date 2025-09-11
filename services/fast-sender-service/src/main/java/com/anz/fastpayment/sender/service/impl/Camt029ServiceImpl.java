package com.anz.fastpayment.sender.service.impl;

import com.anz.fastpayment.sender.service.Camt029Service;
import com.anz.fastpayment.sender.model.Camt029Request;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.stereotype.Service;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathFactory;
import org.w3c.dom.Document;
import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;

@Service
public class Camt029ServiceImpl implements Camt029Service {

    private static final Logger log = LoggerFactory.getLogger(Camt029ServiceImpl.class);

    private final JmsTemplate jmsTemplate;

    @Value("${app.activemq.camt029-queue:camt29.outbound}")
    private String camt029OutboundQueue;

    @Value("${app.sender.max-retry-attempts:5}")
    private int maxRetryAttempts;

    @Value("${app.sender.retry-backoff-ms:1000}")
    private long retryBackoffMs;

    public Camt029ServiceImpl(JmsTemplate jmsTemplate) {
        this.jmsTemplate = jmsTemplate;
    }

    @Override
    public String handleCamt029Request(Camt029Request request) {
        log.info("[CAMT029] Received request for PUID={}, messageType={}, uniqueId={}, error={}",
                safe(request.getPuid()), request.getMessageType(), safe(request.getUniqueId()), safe(request.getError()));

        String xml = buildCamt029Xml(request);
        log.debug("[CAMT029] Built XML (first 500) => {}", preview(xml, 500));

        boolean sent = false;
        String destination = (camt029OutboundQueue == null || camt029OutboundQueue.isBlank()) ? "camt29.outbound" : camt029OutboundQueue;
        for (int attempt = 1; attempt <= Math.max(1, maxRetryAttempts); attempt++) {
            try {
                jmsTemplate.convertAndSend(destination, xml);
                log.info("[AMQ] Sent camt.029 for PUID={} to queue {} (attempt {} of {})", safe(request.getPuid()), destination, attempt, maxRetryAttempts);
                sent = true;
                break;
            } catch (Exception e) {
                log.warn("[AMQ] Send failed for PUID={}, attempt {}/{} err={}", safe(request.getPuid()), attempt, maxRetryAttempts, e.getMessage());
                if (attempt < maxRetryAttempts) {
                    try { Thread.sleep(Math.max(0, retryBackoffMs)); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
                }
            }
        }
        if (!sent) {
            log.warn("[AMQ] Exhausted retries; camt.029 not sent for PUID={}", safe(request.getPuid()));
        }

        return xml;
    }

    private String buildCamt029Xml(Camt029Request req) {
        String now = OffsetDateTime.now().format(DateTimeFormatter.ISO_OFFSET_DATE_TIME);
        String msgId = "C029-" + escape(req.getPuid());
        String orgMsgId = extractOriginalMsgId(req);
        String decision = (req.getError() == null || req.getError().isBlank()) ? "CANC" : "RJCT";
        String addtlInfo = (req.getError() == null || req.getError().isBlank()) ? "Cancellation accepted" : req.getError();
        return "" +
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
                "<Document xmlns=\"urn:iso:std:iso:20022:tech:xsd:camt.029.001.13\">" +
                "<RsltnOfInvstgtn>" +
                "  <Assgnmt>" +
                "    <Id>" + msgId + "</Id>" +
                "    <CreDtTm>" + now + "</CreDtTm>" +
                "  </Assgnmt>" +
                "  <Sts>" +
                "    <AssgnmtCxlConf>" + ("CANC".equals(decision) ? "true" : "false") + "</AssgnmtCxlConf>" +
                "    <Rsn>" +
                "      <Prtry>" + decision + "</Prtry>" +
                "    </Rsn>" +
                "    <AddtlInf>" + escape(addtlInfo) + "</AddtlInf>" +
                "  </Sts>" +
                (orgMsgId.isEmpty() ? "" : ("  <OrgnlInstrId>" + orgMsgId + "</OrgnlInstrId>")) +
                "</RsltnOfInvstgtn>" +
                "</Document>";
    }

    private String extractOriginalMsgId(Camt029Request req) {
        try {
            String original = req.getOriginalXml();
            if (original == null || original.isBlank()) {
                return req.getPuid();
            }
            DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
            dbf.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
            dbf.setFeature("http://xml.org/sax/features/external-general-entities", false);
            dbf.setFeature("http://xml.org/sax/features/external-parameter-entities", false);
            dbf.setExpandEntityReferences(false);
            dbf.setNamespaceAware(true);
            DocumentBuilder db = dbf.newDocumentBuilder();
            Document doc = db.parse(new ByteArrayInputStream(original.getBytes(StandardCharsets.UTF_8)));
            XPath xPath = XPathFactory.newInstance().newXPath();
            // camt.056 OrgnlMsgId | OrgnlInstrId best-effort
            String expr = "//*[local-name()='OrgnlInstrId' or local-name()='OrgnlMsgId']/text()";
            String val = (String) xPath.evaluate(expr, doc, XPathConstants.STRING);
            if (val != null && !val.isBlank()) {
                return escape(val.trim());
            }
        } catch (Exception e) {
            log.warn("[XML] Failed to extract original id from camt.056; falling back to PUID: {}", e.getMessage());
        }
        return req.getPuid();
    }

    private String escape(String s) {
        return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace("\"", "&quot;").replace("'", "&apos;");
    }

    private String preview(String s, int maxChars) {
        if (s == null) return "<empty>";
        String cut = s.substring(0, Math.min(maxChars, s.length()));
        return cut.replaceAll("\\d{8,}", "***masked***");
    }

    private String safe(String s) {
        if (s == null) return "";
        if (s.matches("\\d{8,}")) return "***masked***";
        return s;
    }
}


