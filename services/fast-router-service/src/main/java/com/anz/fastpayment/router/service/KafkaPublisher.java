package com.anz.fastpayment.router.service;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.ClassPathResource;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;

@Service
public class KafkaPublisher {

    private static final Logger log = LoggerFactory.getLogger(KafkaPublisher.class);

    private final KafkaTemplate<String, org.apache.avro.generic.GenericRecord> avroKafkaTemplate;

    @Value("${app.kafka.topics.payment-messages:payment-messages}")
    private String paymentMessagesTopic;

    @Value("${app.kafka.topics.exception-queue:exception-queue}")
    private String exceptionTopic;

    @Value("${app.kafka.topics.pacs002-requests:pacs002-requests}")
    private String pacs002RequestsTopic;

    private final Schema unifiedSchema;

    public KafkaPublisher(KafkaTemplate<String, org.apache.avro.generic.GenericRecord> avroKafkaTemplate) {
        this.avroKafkaTemplate = avroKafkaTemplate;
        try {
            ClassPathResource schemaRes = new ClassPathResource("avro/unified-payment-message.avsc");
            try (InputStream in = schemaRes.getInputStream()) {
                String content = new String(in.readAllBytes(), StandardCharsets.UTF_8);
                this.unifiedSchema = new Schema.Parser().parse(content);
            }
        } catch (Exception e) {
            throw new IllegalStateException("Failed to load Avro schema", e);
        }
    }

    public void publishValidUnified(GenericRecord record, String key) {
        log.info("Publishing avro message to topic {} with key {}", paymentMessagesTopic, key);
        try {
            avroKafkaTemplate.send(paymentMessagesTopic, key, record).get(5, java.util.concurrent.TimeUnit.SECONDS);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            log.warn("Interrupted while publishing to topic={}, key={}", paymentMessagesTopic, key, ie);
        } catch (java.util.concurrent.TimeoutException | java.util.concurrent.ExecutionException e) {
            log.warn("Kafka publish timeout/failure for topic={}, key={}", paymentMessagesTopic, key, e);
        }
    }

    // For backward-compatibility for exception and pacs002 requests (string JSON)
    private final org.springframework.kafka.core.KafkaTemplate<String, String> jsonKafkaTemplate = null;

    public void publishInvalid(String key, String payload) {
        if (jsonKafkaTemplate == null) {
            log.warn("JSON template not configured; skipping invalid publish");
            return;
        }
        try {
            jsonKafkaTemplate.send(exceptionTopic, key, payload).get(5, java.util.concurrent.TimeUnit.SECONDS);
        } catch (Exception e) {
            log.warn("Kafka publish failure for exception topic={}, key={}", exceptionTopic, key, e);
        }
    }

    public void publishPacs002Request(String key, String payload) {
        if (jsonKafkaTemplate == null) {
            log.warn("JSON template not configured; skipping pacs002 request publish");
            return;
        }
        try {
            jsonKafkaTemplate.send(pacs002RequestsTopic, key, payload).get(5, java.util.concurrent.TimeUnit.SECONDS);
        } catch (Exception e) {
            log.warn("Kafka publish failure for pacs002 topic={}, key={}", pacs002RequestsTopic, key, e);
        }
    }

    public Schema getUnifiedSchema() {
        return unifiedSchema;
    }

    private Schema parseSchema(String classpathLocation) throws Exception {
        ClassPathResource res = new ClassPathResource(classpathLocation);
        try (InputStream in = res.getInputStream()) {
            String content = new String(in.readAllBytes(), StandardCharsets.UTF_8);
            return new Schema.Parser().parse(content);
        }
    }
}

