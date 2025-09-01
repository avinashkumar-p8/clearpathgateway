package com.anz.fastpayment.router.service;

import org.apache.kafka.clients.producer.ProducerRecord;
import com.anz.fastpayment.router.model.RouterEvent;
import com.anz.fastpayment.router.repository.RouterEventRepository;
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

    @Value("${app.kafka.topics.payment-events:payment-events}")
    private String paymentEventsTopic;

    public EventPublisher(KafkaTemplate<String, String> kafkaTemplate, RouterEventRepository eventRepository) {
        this.kafkaTemplate = kafkaTemplate;
        this.eventRepository = eventRepository;
    }

    public void publishPaymentReceivedEvent(String puid, String channel, String topicName) {
        String nowTs = DateTimeFormatter.ISO_INSTANT.format(Instant.now());
        String eventId1 = UUID.randomUUID().toString();
        String eventId2 = UUID.randomUUID().toString();
        String json = "{" +
                "\"Header\":{"
                + "\"ComponentName\":\"PSPAPFAFAST\"," 
                + "\"UUID\":\"" + escape(puid) + "\"," 
                + "\"EventInfo\":{"
                + "\"EventCode\":\"P.PSP.STS.M.OP_RPI.100\"," 
                + "\"EventDescription\":\"Payment request received in PSP\"," 
                + "\"EventID\":\"" + eventId2 + "\"," 
                + "\"EventType\":\"PE\"," 
                + "\"EventProducer\":\"Clear Path Gateway\"," 
                + "\"EventTS\":\"" + nowTs + "\"," 
                + "\"EventTopics\":\"" + escape(topicName) + "\"," 
                + "\"SystemId\":null,"
                + "\"Events\":{\"Event\":[{"
                + "\"EventCode\":\"I.PSP.STS.M.OP_RPI.100\",\"EventID\":\"" + eventId1 + "\"},{"
                + "\"EventCode\":\"P.PSP.STS.M.OP_RPI.100\",\"EventID\":\"" + eventId2 + "\"}]},"
                + "\"EventVersion\":null},"
                + "\"ReplyToQueue\":\"PPSP.PPORCH.GPAFL.RSP.01\","
                + "\"ReqMap\":null,"
                + "\"MUID\":\"" + escape(puid) + "\","
                + "\"Channel\":\"" + escape(channel) + "\","
                + "\"Direction\":\"I\","
                + "\"RcvdTS\":\"" + nowTs + "\"," 
                + "\"DomainName\":\"PAYMENTS\",\"DomainType\":\"PAYMENT\"},"
                + "\"Body\":{\"PmtAddRq\":[{}]},"
                + "\"Procctxt\":{\"sideEffect\":[],\"softFail\":[]},"
                + "\"messages\":[{}]}";
        ProducerRecord<String, String> record = new ProducerRecord<>(paymentEventsTopic, puid, json);
        kafkaTemplate.send(record);
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

    private String escape(String s) { return s == null ? "" : s.replace("\\", "\\\\").replace("\"", "\\\""); }
}


