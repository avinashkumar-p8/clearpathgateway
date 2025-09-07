package com.anz.fastpayment.sender.service;

import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

@Service
public class KafkaPublisher implements EventPublisher {

    private static final Logger log = LoggerFactory.getLogger(KafkaPublisher.class);
    private final KafkaTemplate<String, Object> kafkaTemplate;

    public KafkaPublisher(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @Override
    public void publish(String topic, Object payload) {
        CompletableFuture<org.springframework.kafka.support.SendResult<String, Object>> future = kafkaTemplate.send(topic, payload);
        future.whenComplete((result, ex) -> {
            if (ex != null) {
                log.error("[KAFKA] Publish failed to topic={}", topic, ex);
            } else if (result != null && result.getRecordMetadata() != null) {
                RecordMetadata meta = result.getRecordMetadata();
                log.info("[KAFKA] Published to topic={} partition={} offset={}", meta.topic(), meta.partition(), meta.offset());
            } else {
                log.info("[KAFKA] Published to topic={} (no metadata)", topic);
            }
        });
    }
}


