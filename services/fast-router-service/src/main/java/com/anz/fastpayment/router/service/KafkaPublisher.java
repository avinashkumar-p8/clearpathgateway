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

    private final KafkaTemplate<String, String> jsonKafkaTemplate;
    private final KafkaTemplate<String, GenericRecord> avroKafkaTemplate;

    @Value("${app.kafka.topics.payment-messages:payment-messages}")
    private String paymentMessagesTopic;

    @Value("${app.kafka.topics.exception-queue:exception-queue}")
    private String exceptionTopic;

    @Value("${app.kafka.topics.pacs002-requests:pacs002-requests}")
    private String pacs002RequestsTopic;

    private final Schema unifiedSchema;
    private final Schema exceptionSchema;
    private final Schema pacs002RequestSchema;

    public KafkaPublisher(@Qualifier("stringKafkaTemplate") KafkaTemplate<String, String> jsonKafkaTemplate,
                          @Qualifier("avroKafkaTemplate") KafkaTemplate<String, GenericRecord> avroKafkaTemplate) {
        this.jsonKafkaTemplate = jsonKafkaTemplate;
        this.avroKafkaTemplate = avroKafkaTemplate;
        try {
            this.unifiedSchema = parseSchema("avro/unified-payment-message.avsc");
            this.exceptionSchema = parseSchema("avro/router-exception.avsc");
            this.pacs002RequestSchema = parseSchema("avro/pacs002-request.avsc");
        } catch (Exception e) {
            throw new IllegalStateException("Failed to load Avro schema", e);
        }
    }

    public void publishValid(String key, String payload) {
        log.info("Publishing valid message (JSON) to topic {} with key {}", paymentMessagesTopic, key);
        try {
            jsonKafkaTemplate.send(paymentMessagesTopic, key, payload).get(5, java.util.concurrent.TimeUnit.SECONDS);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            log.warn("Interrupted while publishing to topic={}, key={}", paymentMessagesTopic, key, ie);
        } catch (java.util.concurrent.TimeoutException | java.util.concurrent.ExecutionException e) {
            log.warn("Kafka publish timeout/failure for topic={}, key={}", paymentMessagesTopic, key, e);
        }
    }

    public void publishValidUnified(GenericRecord record, String key) {
        log.info("Publishing valid message (Avro) to topic {} with key {}", paymentMessagesTopic, key);
        try {
            avroKafkaTemplate.send(paymentMessagesTopic, key, record).get(5, java.util.concurrent.TimeUnit.SECONDS);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            log.warn("Interrupted while publishing to topic={}, key={}", paymentMessagesTopic, key, ie);
        } catch (java.util.concurrent.TimeoutException | java.util.concurrent.ExecutionException e) {
            log.warn("Kafka publish timeout/failure for topic={}, key={}", paymentMessagesTopic, key, e);
        }
    }

    public void publishInvalid(String key, String payload) {
        log.warn("Publishing invalid message (Avro) to exception topic {} with key {}", exceptionTopic, key);
        try {
            GenericRecord rec = new GenericData.Record(exceptionSchema);
            rec.put("puid", key);
            rec.put("originalXml", payload);
            avroKafkaTemplate.send(exceptionTopic, key, rec).get(5, java.util.concurrent.TimeUnit.SECONDS);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            log.warn("Interrupted while publishing to topic={}, key={}", exceptionTopic, key, ie);
        } catch (java.util.concurrent.TimeoutException | java.util.concurrent.ExecutionException e) {
            log.warn("Kafka publish timeout/failure for topic={}, key={}", exceptionTopic, key, e);
        }
    }

    public void publishPacs002Request(String key, String payload) {
        log.info("Publishing pacs002 request (Avro) to topic {} with key {}", pacs002RequestsTopic, key);
        try {
            GenericRecord rec = new GenericData.Record(pacs002RequestSchema);
            rec.put("puid", key);
            rec.put("payload", payload);
            avroKafkaTemplate.send(pacs002RequestsTopic, key, rec).get(5, java.util.concurrent.TimeUnit.SECONDS);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            log.warn("Interrupted while publishing to topic={}, key={}", pacs002RequestsTopic, key, ie);
        } catch (java.util.concurrent.TimeoutException | java.util.concurrent.ExecutionException e) {
            log.warn("Kafka publish timeout/failure for topic={}, key={}", pacs002RequestsTopic, key, e);
        }
    }

    public Schema getUnifiedSchema() { return unifiedSchema; }

    private Schema parseSchema(String classpathLocation) throws Exception {
        ClassPathResource res = new ClassPathResource(classpathLocation);
        try (InputStream in = res.getInputStream()) {
            String content = new String(in.readAllBytes(), StandardCharsets.UTF_8);
            return new Schema.Parser().parse(content);
        }
    }
}


