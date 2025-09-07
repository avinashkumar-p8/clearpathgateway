package com.anz.fastpayment.sender.messaging;

import com.anz.fastpayment.sender.model.WrapperRequest;
import com.anz.fastpayment.sender.service.EventPublisher;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

@Component
public class WrapperRequestConsumer {

    private static final Logger log = LoggerFactory.getLogger(WrapperRequestConsumer.class);

    private final EventPublisher publisher;
    private final ObjectMapper objectMapper;

    @Value("${app.kafka.topics.sender-inbound:sender-wrapper-requests}")
    private String inboundTopic;

    @Value("${app.kafka.topics.payment-events:payment-events}")
    private String paymentEventsTopic;

    public WrapperRequestConsumer(EventPublisher publisher, ObjectMapper objectMapper) {
        this.publisher = publisher;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(topics = "${app.kafka.topics.sender-inbound:sender-wrapper-requests}", groupId = "fast-sender-service")
    public void onMessage(ConsumerRecord<String, String> record) {
        String payload = record.value();
        if (payload == null) {
            log.warn("[SENDER] Null payload on {} partition={} offset={}", inboundTopic, record.partition(), record.offset());
            return;
        }
        WrapperRequest request;
        try {
            request = objectMapper.readValue(payload, WrapperRequest.class);
        } catch (Exception e) {
            log.warn("[SENDER] Invalid JSON payload on {} partition={} offset={}: {}", inboundTopic, record.partition(), record.offset(), e.getMessage());
            return;
        }
        if (request == null || request.getTrailer() == null || request.getTrailer().getServiceStatus() == null) {
            log.warn("[SENDER] Ignoring invalid wrapper record on {} (missing Trailer.ServiceStatus)", inboundTopic);
            return;
        }
        String puid = java.util.UUID.randomUUID().toString().replace("-", "").substring(0, 16);
        String statusCode = request.getTrailer().getServiceStatus().getStatusCode();
        String statusDesc = request.getTrailer().getServiceStatus().getStatusDesc();

        Map<String, Object> event = new HashMap<>();
        event.put("messageType", "PACS.002");
        event.put("puid", puid);
        event.put("statusCode", statusCode);
        event.put("statusDesc", statusDesc);
        event.put("createdAt", Instant.now().toString());
        event.put("trailer", request.getTrailer());

        publisher.publish(paymentEventsTopic, event);
        log.info("[SENDER] Published PACS.002 for puid={} from inboundTopic={} (offset={})", puid, inboundTopic, record.offset());
    }
}


