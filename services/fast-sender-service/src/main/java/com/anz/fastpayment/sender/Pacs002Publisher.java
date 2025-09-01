package com.anz.fastpayment.sender;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.jms.core.MessagePostProcessor;
import org.springframework.stereotype.Component;
import org.springframework.jms.JmsException;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.jms.Message;

@Component
public class Pacs002Publisher {

    private static final Logger log = LoggerFactory.getLogger(Pacs002Publisher.class);

    private final JmsTemplate jmsTemplate;
    private final String pacs002Queue;
    private final Counter sendSuccessCounter;
    private final Counter sendFailureCounter;

    public Pacs002Publisher(
            JmsTemplate jmsTemplate,
            @Value("${app.activemq.pacs002-queue:pacs002.outbound}") String pacs002Queue,
            MeterRegistry meterRegistry) {
        this.jmsTemplate = jmsTemplate;
        this.pacs002Queue = pacs002Queue;
        this.sendSuccessCounter = meterRegistry.counter("sender.pacs002.send.success");
        this.sendFailureCounter = meterRegistry.counter("sender.pacs002.send.failure");
    }

    public void sendPacs002Xml(String pacs002Xml) {
        if (pacs002Xml == null) {
            log.warn("[SENDER] Null pacs002Xml payload, not sending to queue {}", pacs002Queue);
            return;
        }
        try {
            jmsTemplate.convertAndSend(pacs002Queue, pacs002Xml, (MessagePostProcessor) (Message msg) -> {
                msg.setStringProperty("messageType", "pacs.002");
                return msg;
            });
            sendSuccessCounter.increment();
            log.info("[SENDER] Sent pacs.002 to queue {} (size={} chars)", pacs002Queue, pacs002Xml.length());
        } catch (JmsException ex) {
            sendFailureCounter.increment();
            log.error("[SENDER] Failed sending pacs.002 to queue {}: {}", pacs002Queue, ex.getMessage(), ex);
            // choose not to rethrow to keep flow resilient; service can decide policy here
        }
    }
}


