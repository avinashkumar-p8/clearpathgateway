package com.anz.fastpayment.router.service;

import org.springframework.stereotype.Component;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;

import javax.xml.parsers.DocumentBuilderFactory;
import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;

@Component
public class UniqueIdExtractor {

    public String extractUniqueId(String xml, String messageType) {
        try {
            DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
            dbf.setNamespaceAware(true);
            try {
                dbf.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
                dbf.setFeature("http://xml.org/sax/features/external-general-entities", false);
                dbf.setFeature("http://xml.org/sax/features/external-parameter-entities", false);
                dbf.setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false);
                dbf.setXIncludeAware(false);
                dbf.setExpandEntityReferences(false);
                dbf.setFeature(javax.xml.XMLConstants.FEATURE_SECURE_PROCESSING, true);
                dbf.setAttribute(javax.xml.XMLConstants.ACCESS_EXTERNAL_DTD, "");
                dbf.setAttribute(javax.xml.XMLConstants.ACCESS_EXTERNAL_SCHEMA, "");
            } catch (Exception ignored) { }
            Document doc = dbf.newDocumentBuilder().parse(new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8)));
            // PACS types: prefer EndToEndId, then InstrId
            if (messageType != null && messageType.startsWith("pacs.")) {
                String e2e = firstNonBlank(text(doc, "*", "EndToEndId"), text(doc, null, "EndToEndId"));
                if (notBlank(e2e)) return e2e.trim();
                String instrId = firstNonBlank(text(doc, "*", "InstrId"), text(doc, null, "InstrId"));
                if (notBlank(instrId)) return instrId.trim();
            }
            // camt.056: prefer OrgnlMsgId, then OrgnlInstrId for linkage
            if (messageType != null && messageType.startsWith("camt.056")) {
                String oMsg = firstNonBlank(text(doc, "*", "OrgnlMsgId"), text(doc, null, "OrgnlMsgId"));
                if (notBlank(oMsg)) return oMsg.trim();
                String oInstr = firstNonBlank(text(doc, "*", "OrgnlInstrId"), text(doc, null, "OrgnlInstrId"));
                if (notBlank(oInstr)) return oInstr.trim();
            }
            // Fallback policy: use MsgId (from Group Header)
            String msgId = firstNonBlank(text(doc, "*", "MsgId"), text(doc, null, "MsgId"));
            if (notBlank(msgId)) return msgId.trim();
        } catch (Exception ignore) { }
        // Always non-null: fallback to PUID must be supplied by caller, so return empty to signal fallback
        return "";
    }

    private String text(Document doc, String ns, String local) {
        if (ns == null) {
            NodeList n = doc.getElementsByTagName(local);
            return n.getLength() > 0 ? n.item(0).getTextContent() : null;
        }
        NodeList nodes = "*".equals(ns) ? doc.getElementsByTagNameNS("*", local) : doc.getElementsByTagNameNS(ns, local);
        return nodes.getLength() > 0 ? nodes.item(0).getTextContent() : null;
    }

    private boolean notBlank(String s) { return s != null && !s.isBlank(); }

    private String firstNonBlank(String... values) {
        if (values == null) return null;
        for (String v : values) {
            if (v != null) {
                String t = v.trim();
                if (!t.isEmpty()) return t;
            }
        }
        return null;
    }
}


