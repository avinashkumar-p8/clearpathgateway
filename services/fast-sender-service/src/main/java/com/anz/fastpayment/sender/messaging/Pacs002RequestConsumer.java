package com.anz.fastpayment.sender.messaging;

import com.anz.fastpayment.sender.model.Pacs002Request;
import com.anz.fastpayment.sender.model.Pacs002Response;
import com.anz.fastpayment.sender.service.Pacs002Service;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.JsonProcessingException;

@Component
public class Pacs002RequestConsumer {

    private static final Logger log = LoggerFactory.getLogger(Pacs002RequestConsumer.class);

    private final Pacs002Service pacs002Service;
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Value("${app.kafka.topics.pacs002-requests:pacs002-requests}")
    private String pacs002RequestsTopic;

    public Pacs002RequestConsumer(Pacs002Service pacs002Service) {
        this.pacs002Service = pacs002Service;
    }

    @KafkaListener(topics = "#{'${app.kafka.topics.pacs002-requests:pacs002-requests}'}", groupId = "${spring.application.name}", containerFactory = "byteArrayKafkaListenerContainerFactory")
    public void onMessage(ConsumerRecord<String, byte[]> record) {
        if (record == null) {
            log.warn("[KAFKA] Null record received; skipping");
            return;
        }
        final String topic = record.topic();
        final String key = record.key();
        final byte[] value = record.value();
        if (value == null || value.length == 0) {
            log.warn("[KAFKA] Empty pacs002 request payload; topic={}, key={}", topic, key);
            return;
        }
        try {
            String json;
            if (value.length >= 6 && value[0] == 0) {
                // Confluent framing: try decoding Avro using local schema
                try {
                    org.apache.avro.Schema schema;
                    try (java.io.InputStream in = Pacs002RequestConsumer.class.getClassLoader().getResourceAsStream("avro/pacs002-request.avsc")) {
                        String content = new String(java.util.Objects.requireNonNull(in).readAllBytes(), java.nio.charset.StandardCharsets.UTF_8);
                        schema = new org.apache.avro.Schema.Parser().parse(content);
                    }
                    org.apache.avro.generic.GenericDatumReader<org.apache.avro.generic.GenericRecord> reader = new org.apache.avro.generic.GenericDatumReader<>(schema);
                    org.apache.avro.io.Decoder decoder = org.apache.avro.io.DecoderFactory.get().binaryDecoder(value, 5, value.length - 5, null);
                    org.apache.avro.generic.GenericRecord rec = reader.read(null, decoder);
                    String puid = String.valueOf(rec.get("puid"));
                    String payload = String.valueOf(rec.get("payload"));
                    // Build JSON expected by our model
                    com.fasterxml.jackson.databind.node.ObjectNode node = objectMapper.createObjectNode();
                    node.put("puid", puid);
                    node.put("messageType", "pacs.002.request");
                    node.put("originalXml", payload);
                    json = node.toString();
                } catch (Exception decodeEx) {
                    // Fallback to opaque json
                    byte[] jsonBytes = java.util.Arrays.copyOfRange(value, 5, value.length);
                    json = new String(jsonBytes, java.nio.charset.StandardCharsets.UTF_8);
                }
            } else {
                // Fallback: plain JSON
                json = new String(value, java.nio.charset.StandardCharsets.UTF_8);
            }
            // Normalize JSON to match Pacs002Request contract when fields differ
            try {
                com.fasterxml.jackson.databind.JsonNode node = objectMapper.readTree(json);
                if (node != null) {
                    com.fasterxml.jackson.databind.node.ObjectNode normalized = objectMapper.createObjectNode();
                    String puidVal = node.hasNonNull("puid") ? node.get("puid").asText("") : "";
                    normalized.put("puid", puidVal);
                    // Default messageType if absent
                    String mt = node.hasNonNull("messageType") ? node.get("messageType").asText("") : "pacs.002.request";
                    normalized.put("messageType", mt);
                    // Map payload -> originalXml if originalXml missing
                    String originalXmlVal = node.hasNonNull("originalXml") ? node.get("originalXml").asText("") : (node.hasNonNull("payload") ? node.get("payload").asText("") : "");
                    normalized.put("originalXml", originalXmlVal);
                    // Optional fields
                    if (node.hasNonNull("error")) normalized.put("error", node.get("error").asText(""));
                    if (node.hasNonNull("uniqueId")) normalized.put("uniqueId", node.get("uniqueId").asText(""));
                    json = normalized.toString();
                }
            } catch (Exception ignore) {
                // keep original json
            }
            Pacs002Request req = objectMapper.readValue(json, Pacs002Request.class);
            if (req.getPuid() == null || req.getPuid().isBlank()) {
                log.warn("[KAFKA] Invalid pacs002 request: missing PUID; topic={}, key={}", topic, key);
                return;
            }
            Pacs002Response resp = pacs002Service.handlePacs002Request(req);
            log.info("[PACS002] Accepted request for PUID={}, status={}", resp.getPuid(), resp.getStatus());
        } catch (JsonProcessingException jpe) {
            log.warn("[KAFKA] JSON parse error for pacs002 request; topic={}, key={}, err={}", topic, key, jpe.getOriginalMessage());
        } catch (Exception ex) {
            log.warn("[KAFKA] Failed processing pacs002 request; topic={}, key={}, err={}", topic, key, ex.getMessage(), ex);
        }
    }
}
