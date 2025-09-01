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

    @KafkaListener(topics = "#{'${app.kafka.topics.pacs002-requests:pacs002-requests}'}", groupId = "${spring.application.name}")
    public void onMessage(ConsumerRecord<String, String> record) {
        if (record == null) {
            log.warn("[KAFKA] Null record received; skipping");
            return;
        }
        final String topic = record.topic();
        final String key = record.key();
        final String value = record.value();
        if (value == null || value.isBlank()) {
            log.warn("[KAFKA] Empty pacs002 request payload; topic={}, key={}", topic, key);
            return;
        }
        try {
            Pacs002Request req = objectMapper.readValue(value, Pacs002Request.class);
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
