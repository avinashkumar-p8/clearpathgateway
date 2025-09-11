package com.anz.fastpayment.router.service;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.core.io.ClassPathResource;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.util.ReflectionTestUtils;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class KafkaPublisherTest {

    @Test
    void publishPathsSendToKafka() throws Exception {
        @SuppressWarnings("unchecked")
        KafkaTemplate<String, org.apache.avro.generic.GenericRecord> avroKafka = mock(KafkaTemplate.class);
        when(avroKafka.send(any(), any(), any())).thenReturn(CompletableFuture.completedFuture(null));
        @SuppressWarnings("unchecked")
        ObjectProvider<org.springframework.kafka.core.KafkaTemplate<String, String>> ignored = mock(ObjectProvider.class);

        KafkaPublisher pub = new KafkaPublisher(avroKafka, ignored);
        ReflectionTestUtils.setField(pub, "paymentMessagesTopic", "payment-messages");
        ReflectionTestUtils.setField(pub, "exceptionTopic", "exception-queue");
        ReflectionTestUtils.setField(pub, "pacs002RequestsTopic", "pacs002-requests");

        Schema unified = pub.getUnifiedSchema();
        GenericRecord rec = new GenericData.Record(unified);
        rec.put("messageType", org.apache.avro.generic.GenericData.get().createEnum("PACS_008", unified.getField("messageType").schema()));
        rec.put("messageVersion", "13");
        rec.put("messageId", "K1");
        rec.put("creationDateTime", java.time.Instant.now().toString());
        rec.put("supplementaryData", java.util.Map.of("rawUnifiedJson", "{}"));
        pub.publishValidUnified(rec, "K1");

        pub.publishInvalid("E1", "<xml/>");
        pub.publishPacs002Request("P2", "body");

        verify(avroKafka, atLeast(3)).send(any(), any(), any());
    }
}
