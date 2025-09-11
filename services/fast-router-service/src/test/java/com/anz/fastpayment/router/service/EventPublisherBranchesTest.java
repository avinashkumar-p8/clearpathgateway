package com.anz.fastpayment.router.service;

import com.anz.fastpayment.router.repository.RouterEventRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.util.ReflectionTestUtils;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class EventPublisherBranchesTest {

    @Test
    void handlesKafkaSendFailureGracefully() throws Exception {
        @SuppressWarnings("unchecked")
        KafkaTemplate<String, GenericRecord> tpl = mock(KafkaTemplate.class);
        doThrow(new RuntimeException("send fail")).when(tpl).send(any(ProducerRecord.class));
        RouterEventRepository repo = mock(RouterEventRepository.class);
        EventPublisher pub = new EventPublisher(tpl, repo, new ObjectMapper());
        ReflectionTestUtils.setField(pub, "paymentEventsTopic", "payment-events");
        assertDoesNotThrow(() -> pub.publishPaymentReceivedEvent("P3", "G3I", "payment-messages"));
    }
}
