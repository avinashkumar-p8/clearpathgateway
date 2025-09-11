package com.anz.fastpayment.router.service;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.concurrent.CompletableFuture;

import static org.mockito.Mockito.*;

class KafkaPublisherBranchesTest {

    @Test
    void publishInvalidAndPacs002HandleFailures() throws Exception {
        @SuppressWarnings("unchecked")
        KafkaTemplate<String, org.apache.avro.generic.GenericRecord> avroKafka = mock(KafkaTemplate.class);
        when(avroKafka.send(any(), any(), any())).thenThrow(new RuntimeException("send fail"))
                .thenReturn(CompletableFuture.completedFuture(null));
        @SuppressWarnings("unchecked")
        ObjectProvider<org.springframework.kafka.core.KafkaTemplate<String, String>> ignored = mock(ObjectProvider.class);
        KafkaPublisher pub = new KafkaPublisher(avroKafka, ignored);
        org.springframework.test.util.ReflectionTestUtils.setField(pub, "paymentMessagesTopic", "payment-messages");
        org.springframework.test.util.ReflectionTestUtils.setField(pub, "exceptionTopic", "exception-queue");
        org.springframework.test.util.ReflectionTestUtils.setField(pub, "pacs002RequestsTopic", "pacs002-requests");

        pub.publishInvalid("K1", "<xml/>"); // should not throw, only warn
        pub.publishPacs002Request("K2", "{}");
        verify(avroKafka, atLeast(2)).send(any(), any(), any());
    }

    @Test
    void publishValidUnifiedTimeoutAndInterruptedHandled() {
        @SuppressWarnings("unchecked")
        KafkaTemplate<String, org.apache.avro.generic.GenericRecord> avroKafka = mock(KafkaTemplate.class);
        // First call returns a never-completing future -> timeout branch
        CompletableFuture<Object> never = new CompletableFuture<>();
        when(avroKafka.send(any(), any(), any())).thenReturn((CompletableFuture) never);
        @SuppressWarnings("unchecked")
        ObjectProvider<org.springframework.kafka.core.KafkaTemplate<String, String>> ignored = mock(ObjectProvider.class);
        KafkaPublisher pub = new KafkaPublisher(avroKafka, ignored);
        org.springframework.test.util.ReflectionTestUtils.setField(pub, "paymentMessagesTopic", "payment-messages");

        Schema unified = pub.getUnifiedSchema();
        GenericRecord rec = new GenericData.Record(unified);
        rec.put("messageType", org.apache.avro.generic.GenericData.get().createEnum("PACS_003", unified.getField("messageType").schema()));
        rec.put("messageVersion", "11");
        rec.put("messageId", "K1");
        rec.put("creationDateTime", java.time.Instant.now().toString());
        rec.put("supplementaryData", java.util.Map.of("rawUnifiedJson", "{}"));
        pub.publishValidUnified(rec, "K1");

        // Second call: interrupt current thread to trigger InterruptedException path
        Thread.currentThread().interrupt();
        pub.publishValidUnified(rec, "K1");
        // clear interrupt for subsequent tests
        Thread.interrupted();

        verify(avroKafka, atLeast(2)).send(any(), any(), any());
    }
}
