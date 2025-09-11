package com.anz.fastpayment.router.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import static org.junit.jupiter.api.Assertions.*;

class KafkaTopicConfigTest {

    @Test
    void createsTopics() {
        KafkaTopicConfig c = new KafkaTopicConfig();
        ReflectionTestUtils.setField(c, "paymentMessagesTopic", "payment-messages");
        ReflectionTestUtils.setField(c, "exceptionTopic", "exception-queue");
        ReflectionTestUtils.setField(c, "bankAvailabilityTopic", "bank-availability");
        ReflectionTestUtils.setField(c, "pacs002RequestsTopic", "pacs002-requests");
        assertTopic(c.paymentMessagesTopic());
        assertTopic(c.exceptionQueueTopic());
        assertTopic(c.bankAvailabilityTopic());
        assertTopic(c.pacs002RequestsTopic());
    }

    private static void assertTopic(NewTopic t) {
        assertNotNull(t);
        assertNotNull(t.name());
        assertTrue(t.numPartitions() > 0);
        assertTrue(t.replicationFactor() > 0);
    }
}
