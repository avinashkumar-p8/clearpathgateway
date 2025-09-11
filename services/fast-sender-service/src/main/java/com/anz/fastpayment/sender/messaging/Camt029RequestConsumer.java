package com.anz.fastpayment.sender.messaging;

import com.anz.fastpayment.sender.model.Camt029Request;
import com.anz.fastpayment.sender.service.Camt029Service;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class Camt029RequestConsumer {

    private static final Logger log = LoggerFactory.getLogger(Camt029RequestConsumer.class);

    private final Camt029Service camt029Service;
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Value("${app.kafka.topics.camt029-requests:camt029-requests}")
    private String camt029RequestsTopic;

    public Camt029RequestConsumer(Camt029Service camt029Service) {
        this.camt029Service = camt029Service;
    }

    @KafkaListener(topics = "#{'${app.kafka.topics.camt029-requests:camt029-requests}'}", groupId = "${spring.application.name}", containerFactory = "byteArrayKafkaListenerContainerFactory")
    public void onMessage(ConsumerRecord<String, byte[]> record) {
        if (record == null) {
            log.warn("[KAFKA] Null record received; skipping");
            return;
        }
        final String topic = record.topic();
        final String key = record.key();
        final byte[] value = record.value();
        if (value == null || value.length == 0) {
            log.warn("[KAFKA] Empty camt029 request payload; topic={}, key={}", topic, key);
            return;
        }
        try {
            String json;
            if (value.length >= 6 && value[0] == 0) {
                // Confluent framing: try decoding Avro using local schema if added in future; else slice
                try {
                    byte[] jsonBytes = java.util.Arrays.copyOfRange(value, 5, value.length);
                    json = new String(jsonBytes, java.nio.charset.StandardCharsets.UTF_8);
                } catch (Exception decodeEx) {
                    json = new String(value, java.nio.charset.StandardCharsets.UTF_8);
                }
            } else {
                json = new String(value, java.nio.charset.StandardCharsets.UTF_8);
            }
            // Normalize fields similar to Pacs002 path
            try {
                JsonNode node = objectMapper.readTree(json);
                if (node != null) {
                    ObjectNode normalized = objectMapper.createObjectNode();
                    String puidVal = node.hasNonNull("puid") ? node.get("puid").asText("") : "";
                    normalized.put("puid", puidVal);
                    String mt = node.hasNonNull("messageType") ? node.get("messageType").asText("") : "camt.056.001.11";
                    normalized.put("messageType", mt);
                    String originalXmlVal = node.hasNonNull("originalXml") ? node.get("originalXml").asText("") : (node.hasNonNull("payload") ? node.get("payload").asText("") : "");
                    normalized.put("originalXml", originalXmlVal);
                    if (node.hasNonNull("error")) normalized.put("error", node.get("error").asText(""));
                    if (node.hasNonNull("uniqueId")) normalized.put("uniqueId", node.get("uniqueId").asText(""));
                    json = normalized.toString();
                }
            } catch (Exception ignore) { }

            Camt029Request req = objectMapper.readValue(json, Camt029Request.class);
            if (req.getPuid() == null || req.getPuid().isBlank()) {
                log.warn("[KAFKA] Invalid camt029 request: missing PUID; topic={}, key={}", topic, key);
                return;
            }
            camt029Service.handleCamt029Request(req);
            log.info("[CAMT029] Accepted request for PUID={}", req.getPuid());
        } catch (JsonProcessingException jpe) {
            log.warn("[KAFKA] JSON parse error for camt029 request; topic={}, key={}, err={}", topic, key, jpe.getOriginalMessage());
        } catch (Exception ex) {
            log.warn("[KAFKA] Failed processing camt029 request; topic={}, key={}, err={}", topic, key, ex.getMessage(), ex);
        }
    }
}


