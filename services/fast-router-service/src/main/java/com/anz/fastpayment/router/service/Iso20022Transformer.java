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
        if ("camt.029.001.13".equals(messageType)) {
            return transformCamt029(xml, puid);
        }
        if ("head.001.001.01".equals(messageType)) {
            return transformHead001(xml, puid);
        }
        // passthrough for unsupported types for now
        return "{\"puid\":\"" + puid + "\",\"raw\":" + quote(jsonEscape(xml)) + "}";
    }

    private String transformPacs008(String xml, String puid) throws Exception {
        Document doc = parseSecure(xml);

        String ns = "urn:iso:std:iso:20022:tech:xsd:pacs.008.001.13";
        String msgId = text(doc, ns, "MsgId");
        String creDtTm = text(doc, ns, "CreDtTm");
        String e2e = text(doc, ns, "EndToEndId");
        String amt = text(doc, ns, "IntrBkSttlmAmt");
        String ccy = attr(doc, ns, "IntrBkSttlmAmt", "Ccy");

        com.fasterxml.jackson.databind.node.ObjectNode root = objectMapper.createObjectNode();
        com.fasterxml.jackson.databind.node.ObjectNode header = root.putObject("Header");
        header.put("ComponentName", "PSPAPFAFAST");
        header.put("UUID", puid);
        header.put("Channel", "G3I");
        header.put("Direction", "I");
        com.fasterxml.jackson.databind.node.ObjectNode eventInfo = header.putObject("EventInfo");
        eventInfo.put("EventCode", "P.PSP.STS.M.OP_RPI.100");
        eventInfo.put("EventDescription", "Payment request received in PSP");
        eventInfo.put("EventID", java.util.UUID.randomUUID().toString());
        eventInfo.put("EventType", "PE");
        eventInfo.put("EventProducer", "Clear Path Gateway");
        eventInfo.put("EventTS", java.time.format.DateTimeFormatter.ISO_INSTANT.format(java.time.Instant.now()));
        eventInfo.put("EventTopics", "payment-messages");
        com.fasterxml.jackson.databind.node.ObjectNode events = eventInfo.putObject("Events");
        com.fasterxml.jackson.databind.node.ArrayNode arr = events.putArray("Event");
        com.fasterxml.jackson.databind.node.ObjectNode e1 = arr.addObject();
        e1.put("EventCode", "I.PSP.STS.M.OP_RPI.100");
        e1.put("EventID", java.util.UUID.randomUUID().toString());
        com.fasterxml.jackson.databind.node.ObjectNode e2node = arr.addObject();
        e2node.put("EventCode", "P.PSP.STS.M.OP_RPI.100");
        e2node.put("EventID", eventInfo.get("EventID").asText());

        com.fasterxml.jackson.databind.node.ObjectNode body = root.putObject("Body");
        com.fasterxml.jackson.databind.node.ArrayNode pmtAddRq = body.putArray("PmtAddRq");
        com.fasterxml.jackson.databind.node.ObjectNode first = pmtAddRq.addObject();
        first.put("RqUID", msgId != null ? msgId : puid);
        com.fasterxml.jackson.databind.node.ObjectNode msgHdr = first.putObject("MsgHdr");
        if (creDtTm != null) msgHdr.put("ClientDt", creDtTm);
        msgHdr.put("ClientName", "G3I");
        com.fasterxml.jackson.databind.node.ObjectNode payHdr = first.putObject("PayHdr");
        payHdr.put("PODsID", puid);
        if (e2e != null) payHdr.put("PaymentID", e2e);
        com.fasterxml.jackson.databind.node.ObjectNode toAcct = first.putObject("ToAcct");
        if (ccy != null) toAcct.put("CurCode", ccy);
        if (amt != null) {
            try { toAcct.put("Amount", Double.parseDouble(amt)); } catch (Exception ignore) { toAcct.put("Amount", amt); }
        }

        root.putObject("Trailer").putObject("ServiceStatus").put("StatusCode", "OK");
        root.putObject("Procctxt");
        root.putArray("messages");
        return objectMapper.writeValueAsString(root);
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

    private String transformCamt029(String xml, String puid) throws Exception {
        Document doc = parseSecure(xml);

        String ns = "urn:iso:std:iso:20022:tech:xsd:camt.029.001.13";
        String caseId = text(doc, ns, "Id"); // Case/Id under RsltnOfInvstgtn/RslvdCase or ResolutionData5
        String msgId = text(doc, ns, "Id"); // CaseAssignment/Id
        String creDtTm = text(doc, ns, "CreDtTm");

        StringBuilder sb = new StringBuilder();
        sb.append("{");
        sb.append("\"puid\":\"").append(escape(puid)).append("\",");
        sb.append("\"messageType\":\"CAMT_029\",");
        sb.append("\"messageVersion\":\"13\",");
        if (msgId != null) sb.append("\"messageId\":\"").append(escape(msgId)).append("\",");
        if (creDtTm != null) sb.append("\"creationDateTime\":\"").append(escape(creDtTm)).append("\",");
        if (caseId != null) sb.append("\"caseId\":\"").append(escape(caseId)).append("\",");
        if (sb.charAt(sb.length()-1) == ',') sb.setLength(sb.length()-1);
        sb.append("}");
        return sb.toString();
    }

    private String transformHead001(String xml, String puid) throws Exception {
        Document doc = parseSecure(xml);
        String ns = "urn:iso:std:iso:20022:tech:xsd:head.001.001.01";
        String msgId = text(doc, ns, "BizMsgIdr");
        String creDt = text(doc, ns, "CreDt");
        String id = text(doc, ns, "Id");

        StringBuilder sb = new StringBuilder();
        sb.append("{");
        sb.append("\"puid\":\"").append(escape(puid)).append("\",");
        sb.append("\"messageType\":\"HEAD_001\",");
        sb.append("\"messageVersion\":\"01\",");
        if (msgId != null) sb.append("\"messageId\":\"").append(escape(msgId)).append("\",");
        if (creDt != null) sb.append("\"creationDateTime\":\"").append(escape(creDt)).append("\",");
        if (id != null) sb.append("\"headerId\":\"").append(escape(id)).append("\",");
        if (sb.charAt(sb.length()-1) == ',') sb.setLength(sb.length()-1);
        sb.append("}");
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


