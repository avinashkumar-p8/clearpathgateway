package com.anz.fastpayment.router.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class KafkaPublisher {

    private static final Logger log = LoggerFactory.getLogger(KafkaPublisher.class);

    private final KafkaTemplate<String, String> kafkaTemplate;

    @Value("${app.kafka.topics.payment-messages:payment-messages}")
    private String paymentMessagesTopic;

    @Value("${app.kafka.topics.exception-queue:exception-queue}")
    private String exceptionTopic;

    @Value("${app.kafka.topics.pacs002-requests:pacs002-requests}")
    private String pacs002RequestsTopic;

    public KafkaPublisher(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void publishValid(String key, String payload) {
        log.info("Publishing valid message to topic {} with key {}", paymentMessagesTopic, key);
        try {
            kafkaTemplate.send(paymentMessagesTopic, key, payload).get(5, java.util.concurrent.TimeUnit.SECONDS);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            log.warn("Interrupted while publishing to topic={}, key={}", paymentMessagesTopic, key, ie);
        } catch (java.util.concurrent.TimeoutException | java.util.concurrent.ExecutionException e) {
            log.warn("Kafka publish timeout/failure for topic={}, key={}", paymentMessagesTopic, key, e);
        }
    }

    public void publishInvalid(String key, String payload) {
        log.warn("Publishing invalid message to exception topic {} with key {}", exceptionTopic, key);
        try {
            kafkaTemplate.send(exceptionTopic, key, payload).get(5, java.util.concurrent.TimeUnit.SECONDS);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            log.warn("Interrupted while publishing to topic={}, key={}", exceptionTopic, key, ie);
        } catch (java.util.concurrent.TimeoutException | java.util.concurrent.ExecutionException e) {
            log.warn("Kafka publish timeout/failure for topic={}, key={}", exceptionTopic, key, e);
        }
    }

    public void publishPacs002Request(String key, String payload) {
        log.info("Publishing pacs002 request to topic {} with key {}", pacs002RequestsTopic, key);
        try {
            kafkaTemplate.send(pacs002RequestsTopic, key, payload).get(5, java.util.concurrent.TimeUnit.SECONDS);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            log.warn("Interrupted while publishing to topic={}, key={}", pacs002RequestsTopic, key, ie);
        } catch (java.util.concurrent.TimeoutException | java.util.concurrent.ExecutionException e) {
            log.warn("Kafka publish timeout/failure for topic={}, key={}", pacs002RequestsTopic, key, e);
        }
    }
}


