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
            if ("pacs.008.001.13".equals(messageType)) {
                String e2e = firstNonBlank(
                        text(doc, "urn:iso:std:iso:20022:tech:xsd:pacs.008.001.13", "EndToEndId"),
                        text(doc, "*", "EndToEndId"),
                        text(doc, null, "EndToEndId")
                );
                if (notBlank(e2e)) return e2e.trim();
                String instr = firstNonBlank(
                        text(doc, "urn:iso:std:iso:20022:tech:xsd:pacs.008.001.13", "InstrId"),
                        text(doc, "*", "InstrId"),
                        text(doc, null, "InstrId")
                );
                if (notBlank(instr)) return instr.trim();
                String tx = firstNonBlank(
                        text(doc, "urn:iso:std:iso:20022:tech:xsd:pacs.008.001.13", "TxId"),
                        text(doc, "*", "TxId"),
                        text(doc, null, "TxId")
                );
                if (notBlank(tx)) return tx.trim();
            }
            if (messageType != null && messageType.startsWith("pacs.002")) {
                String e2e = firstNonBlank(text(doc, "*", "OrgnlEndToEndId"), text(doc, null, "OrgnlEndToEndId"));
                if (notBlank(e2e)) return e2e.trim();
                String tx = firstNonBlank(text(doc, "*", "OrgnlTxId"), text(doc, null, "OrgnlTxId"));
                if (notBlank(tx)) return tx.trim();
                String mid = firstNonBlank(text(doc, "*", "OrgnlMsgId"), text(doc, null, "OrgnlMsgId"));
                if (notBlank(mid)) return mid.trim();
            }
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


