package com.anz.fastpayment.router.service;

import com.anz.fastpayment.router.model.RouterEvent;
import com.anz.fastpayment.router.repository.RouterEventRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.UUID;

@Component
public class EventPublisher {

    private static final Logger log = LoggerFactory.getLogger(EventPublisher.class);

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final RouterEventRepository eventRepository;
    private final ObjectMapper objectMapper;

    @Value("${app.kafka.topics.payment-events:payment-events}")
    private String paymentEventsTopic;

    public EventPublisher(KafkaTemplate<String, String> kafkaTemplate,
                          RouterEventRepository eventRepository,
                          ObjectMapper objectMapper) {
        this.kafkaTemplate = kafkaTemplate;
        this.eventRepository = eventRepository;
        this.objectMapper = objectMapper;
    }

    public void publishPaymentReceivedEvent(String puid, String channel, String topicName) {
        String nowTs = DateTimeFormatter.ISO_INSTANT.format(Instant.now());
        String eventId1 = UUID.randomUUID().toString();
        String eventId2 = UUID.randomUUID().toString();

        ObjectNode root = objectMapper.createObjectNode();
        ObjectNode header = root.putObject("Header");
        header.put("ComponentName", "PSPAPFAFAST");
        header.put("UUID", puid == null ? "" : puid);
        ObjectNode eventInfo = header.putObject("EventInfo");
        eventInfo.put("EventCode", "P.PSP.STS.M.OP_RPI.100");
        eventInfo.put("EventDescription", "Payment request received in PSP");
        eventInfo.put("EventID", eventId2);
        eventInfo.put("EventType", "PE");
        eventInfo.put("EventProducer", "Clear Path Gateway");
        eventInfo.put("EventTS", nowTs);
        eventInfo.put("EventTopics", topicName);
        eventInfo.putNull("SystemId");
        ObjectNode events = eventInfo.putObject("Events");
        ArrayNode eventArray = events.putArray("Event");
        ObjectNode e1 = objectMapper.createObjectNode();
        e1.put("EventCode", "I.PSP.STS.M.OP_RPI.100");
        e1.put("EventID", eventId1);
        ObjectNode e2 = objectMapper.createObjectNode();
        e2.put("EventCode", "P.PSP.STS.M.OP_RPI.100");
        e2.put("EventID", eventId2);
        eventArray.add(e1).add(e2);
        eventInfo.putNull("EventVersion");

        header.put("ReplyToQueue", "PPSP.PPORCH.GPAFL.RSP.01");
        header.putNull("ReqMap");
        header.put("MUID", puid == null ? "" : puid);
        header.put("Channel", channel == null ? "" : channel);
        header.put("Direction", "I");
        header.put("RcvdTS", nowTs);
        header.put("DomainName", "PAYMENTS");
        header.put("DomainType", "PAYMENT");

        ObjectNode body = root.putObject("Body");
        body.putArray("PmtAddRq").add(objectMapper.createObjectNode());

        ObjectNode proc = root.putObject("Procctxt");
        proc.putArray("sideEffect");
        proc.putArray("softFail");

        root.putArray("messages").add(objectMapper.createObjectNode());

        String json;
        try {
            json = objectMapper.writeValueAsString(root);
        } catch (Exception ex) {
            log.error("[EVENT] Failed to serialize payment event PUID={}", puid, ex);
            return;
        }

        ProducerRecord<String, String> record = new ProducerRecord<>(paymentEventsTopic, puid, json);
        try {
            kafkaTemplate.send(record);
        } catch (Exception ex) {
            log.warn("[EVENT] Kafka send failed for PUID={} topic={}", puid, paymentEventsTopic, ex);
        }
        try {
            if (eventRepository != null) {
                RouterEvent ev = new RouterEvent();
                ev.setPuid(puid);
                ev.setTopic(paymentEventsTopic);
                ev.setCreatedAt(Instant.now());
                ev.setJson(json);
                eventRepository.save(ev);
            }
        } catch (Exception ignore) { }
        log.info("[EVENT] Published payment received event for PUID={} to topic {}", puid, paymentEventsTopic);
    }
}


