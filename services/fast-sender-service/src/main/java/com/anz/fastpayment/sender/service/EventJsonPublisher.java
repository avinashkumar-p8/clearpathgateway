package com.anz.fastpayment.sender.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import java.time.Duration;

@Service
public class EventJsonPublisher {

    private static final Logger log = LoggerFactory.getLogger(EventJsonPublisher.class);

    private final KafkaTemplate<String, String> kafkaTemplate;

    @Value("${app.kafka.topics.payment-events:payment-events}")
    private String paymentEventsTopic;

    public EventJsonPublisher(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void publish(String key, String payload) {
        try {
            kafkaTemplate.send(paymentEventsTopic, key, payload).get(5, java.util.concurrent.TimeUnit.SECONDS);
            log.info("[KAFKA] Published event JSON for key={} to topic {}", key, paymentEventsTopic);
        } catch (Exception e) {
            log.warn("[KAFKA] Publish timeout/failure for key={}, err={}", key, e.getMessage());
        }
    }
}
