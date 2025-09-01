package com.anz.fastpayment.router.service;

import com.anz.fastpayment.router.mapping.TransformationConfigLoader;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;

/**
 * XML -> Unified JSON transformer using the mapping model in resources/mappings/transformation-config.json.
 * Starts with key fields and is structured to expand coverage by following the config tree.
 */
import org.springframework.stereotype.Component;

@Component
public class Iso20022Transformer {

    private final TransformationConfigLoader configLoader;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public Iso20022Transformer(TransformationConfigLoader configLoader) {
        this.configLoader = configLoader;
    }

    public String toUnifiedJson(String xml, String messageType, String puid) throws Exception {
        if ("pacs.008.001.13".equals(messageType)) {
            return transformPacs008(xml, puid);
        }
        if ("pacs.003.001.11".equals(messageType)) {
            return transformPacs003(xml, puid);
        }
        if ("pacs.007.001.13".equals(messageType)) {
            return transformPacs007(xml, puid);
        }
        if ("camt.056.001.11".equals(messageType)) {
            return transformCamt056(xml, puid);
        }
        // passthrough for unsupported types for now
        return "{\"puid\":\"" + puid + "\",\"raw\":" + quote(jsonEscape(xml)) + "}";
    }

    private String transformPacs008(String xml, String puid) throws Exception {
        Document doc = parseSecure(xml);

        // naive extraction for demo: MsgId, CreDtTm, EndToEndId, IntrBkSttlmAmt/@Ccy and text
        String ns = "urn:iso:std:iso:20022:tech:xsd:pacs.008.001.13";
        String msgId = text(doc, ns, "MsgId");
        String creDtTm = text(doc, ns, "CreDtTm");
        String e2e = text(doc, ns, "EndToEndId");
        String amt = text(doc, ns, "IntrBkSttlmAmt");
        String ccy = attr(doc, ns, "IntrBkSttlmAmt", "Ccy");

        StringBuilder sb = new StringBuilder();
        sb.append("{");
        sb.append("\"puid\":\"").append(escape(puid)).append("\",");
        sb.append("\"messageType\":\"PACS_008\",");
        sb.append("\"messageVersion\":\"13\",");
        if (msgId != null) sb.append("\"messageId\":\"").append(escape(msgId)).append("\",");
        if (creDtTm != null) sb.append("\"creationDateTime\":\"").append(escape(creDtTm)).append("\",");
        sb.append("\"transactions\":[{");
        if (e2e != null) sb.append("\"endToEndId\":\"").append(escape(e2e)).append("\",");
        if (amt != null) sb.append("\"amount\":").append(amt).append(",");
        if (ccy != null) sb.append("\"currency\":\"").append(escape(ccy)).append("\",");
        // trim trailing comma if present
        if (sb.charAt(sb.length()-1) == ',') sb.setLength(sb.length()-1);
        sb.append("}]}");
        return sb.toString();
    }

    private String transformPacs003(String xml, String puid) throws Exception {
        Document doc = parseSecure(xml);

        String ns = "urn:iso:std:iso:20022:tech:xsd:pacs.003.001.11";
        String msgId = text(doc, ns, "MsgId");
        String creDtTm = text(doc, ns, "CreDtTm");
        // DrctDbtTxInf
        String e2e = text(doc, ns, "EndToEndId");
        String instrAmt = text(doc, ns, "InstdAmt");
        String instrCcy = attr(doc, ns, "InstdAmt", "Ccy");

        StringBuilder sb = new StringBuilder();
        sb.append("{");
        sb.append("\"puid\":\"").append(escape(puid)).append("\",");
        sb.append("\"messageType\":\"PACS_003\",");
        sb.append("\"messageVersion\":\"11\",");
        if (msgId != null) sb.append("\"messageId\":\"").append(escape(msgId)).append("\",");
        if (creDtTm != null) sb.append("\"creationDateTime\":\"").append(escape(creDtTm)).append("\",");
        sb.append("\"transactions\":[{");
        if (e2e != null) sb.append("\"endToEndId\":\"").append(escape(e2e)).append("\",");
        if (instrAmt != null) sb.append("\"amount\":").append(instrAmt).append(",");
        if (instrCcy != null) sb.append("\"currency\":\"").append(escape(instrCcy)).append("\",");
        if (sb.charAt(sb.length()-1) == ',') sb.setLength(sb.length()-1);
        sb.append("}]}\n");
        return sb.toString();
    }

    private String transformPacs007(String xml, String puid) throws Exception {
        Document doc = parseSecure(xml);

        String ns = "urn:iso:std:iso:20022:tech:xsd:pacs.007.001.13";
        String msgId = text(doc, ns, "MsgId");
        String creDtTm = text(doc, ns, "CreDtTm");
        String orgMsgId = text(doc, ns, "OrgnlMsgId");
        String rvsdAmt = text(doc, ns, "RvsdIntrBkSttlmAmt");
        String rvsdCcy = attr(doc, ns, "RvsdIntrBkSttlmAmt", "Ccy");
        String reversalId = text(doc, ns, "RvslId");

        StringBuilder sb = new StringBuilder();
        sb.append("{");
        sb.append("\"puid\":\"").append(escape(puid)).append("\",");
        sb.append("\"messageType\":\"PACS_007\",");
        sb.append("\"messageVersion\":\"13\",");
        if (msgId != null) sb.append("\"messageId\":\"").append(escape(msgId)).append("\",");
        if (creDtTm != null) sb.append("\"creationDateTime\":\"").append(escape(creDtTm)).append("\",");
        if (orgMsgId != null) sb.append("\"originalMessageId\":\"").append(escape(orgMsgId)).append("\",");
        sb.append("\"transactions\":[{");
        if (reversalId != null) sb.append("\"reversalId\":\"").append(escape(reversalId)).append("\",");
        if (rvsdAmt != null) sb.append("\"amount\":").append(rvsdAmt).append(",");
        if (rvsdCcy != null) sb.append("\"currency\":\"").append(escape(rvsdCcy)).append("\",");
        if (sb.charAt(sb.length()-1) == ',') sb.setLength(sb.length()-1);
        sb.append("}]}\n");
        return sb.toString();
    }

    private String transformCamt056(String xml, String puid) throws Exception {
        Document doc = parseSecure(xml);

        String ns = "urn:iso:std:iso:20022:tech:xsd:camt.056.001.11";
        String id = text(doc, ns, "Id"); // Case/Id
        String creDtTm = text(doc, ns, "CreDtTm");
        String orgMsgId = text(doc, ns, "OrgnlMsgId");

        StringBuilder sb = new StringBuilder();
        sb.append("{");
        sb.append("\"puid\":\"").append(escape(puid)).append("\",");
        sb.append("\"messageType\":\"CAMT_056\",");
        sb.append("\"messageVersion\":\"11\",");
        if (id != null) sb.append("\"caseId\":\"").append(escape(id)).append("\",");
        if (orgMsgId != null) sb.append("\"originalMessageId\":\"").append(escape(orgMsgId)).append("\",");
        if (creDtTm != null) sb.append("\"creationDateTime\":\"").append(escape(creDtTm)).append("\",");
        sb.append("\"transactions\":[{}]}");
        return sb.toString();
    }

    private String text(Document doc, String ns, String localName) {
        NodeList nodes = doc.getElementsByTagNameNS(ns, localName);
        return nodes.getLength() > 0 ? nodes.item(0).getTextContent() : null;
    }

    private String attr(Document doc, String ns, String localName, String attr) {
        NodeList nodes = doc.getElementsByTagNameNS(ns, localName);
        return nodes.getLength() > 0 && nodes.item(0).getAttributes() != null && nodes.item(0).getAttributes().getNamedItem(attr) != null
                ? nodes.item(0).getAttributes().getNamedItem(attr).getTextContent() : null;
    }

    private String escape(String s) {
        return s.replace("\\", "\\\\").replace("\"", "\\\"");
    }

    private String jsonEscape(String s) {
        return s.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n");
    }

    private String quote(String s) { return "\"" + s + "\""; }

    private Document parseSecure(String xml) throws Exception {
        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        dbf.setNamespaceAware(true);
        try {
            dbf.setFeature(javax.xml.XMLConstants.FEATURE_SECURE_PROCESSING, true);
            dbf.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
            dbf.setFeature("http://xml.org/sax/features/external-general-entities", false);
            dbf.setFeature("http://xml.org/sax/features/external-parameter-entities", false);
        } catch (Exception ignore) { /* best-effort hardening */ }
        dbf.setXIncludeAware(false);
        dbf.setExpandEntityReferences(false);
        DocumentBuilder db = dbf.newDocumentBuilder();
        return db.parse(new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8)));
    }
}


