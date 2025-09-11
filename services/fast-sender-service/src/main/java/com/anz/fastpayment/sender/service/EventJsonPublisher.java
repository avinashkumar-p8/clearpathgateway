package com.anz.fastpayment.sender.service;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import java.time.Duration;

@Service
public class EventJsonPublisher {

    private static final Logger log = LoggerFactory.getLogger(EventJsonPublisher.class);

    private final KafkaTemplate<String, GenericRecord> kafkaTemplate;
    private final Schema eventSchema;

    @Value("${app.kafka.topics.payment-events:payment-events}")
    private String paymentEventsTopic;

    public EventJsonPublisher(KafkaTemplate<String, GenericRecord> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
        try {
            String schemaJson = new String(
                    java.util.Objects.requireNonNull(
                            EventJsonPublisher.class.getClassLoader().getResourceAsStream("avro/payment-event.avsc")
                    ).readAllBytes(),
                    java.nio.charset.StandardCharsets.UTF_8
            );
            this.eventSchema = new Schema.Parser().parse(schemaJson);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to load payment-event.avsc", e);
        }
    }

    public void publish(String key, String payload) {
        try {
            GenericRecord rec = new GenericData.Record(eventSchema);
            rec.put("puid", key);
            rec.put("json", payload);
            kafkaTemplate.send(paymentEventsTopic, key, rec).get(5, java.util.concurrent.TimeUnit.SECONDS);
            log.info("[KAFKA] Published event JSON for key={} to topic {}", key, paymentEventsTopic);
        } catch (Exception e) {
            log.warn("[KAFKA] Publish timeout/failure for key={}, err={}", key, e.getMessage());
        }
    }
}
