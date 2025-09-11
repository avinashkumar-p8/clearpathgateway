package com.anz.fastpayment.router.service;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class KafkaPublisherSuccessTest {

    private KafkaTemplate<String, org.apache.avro.generic.GenericRecord> avroTemplate;
    private KafkaPublisher publisher;

    @BeforeEach
    void setUp() {
        avroTemplate = mock(KafkaTemplate.class);
        publisher = new KafkaPublisher(avroTemplate, null);
        ReflectionTestUtils.setField(publisher, "paymentMessagesTopic", "payment-messages");
        ReflectionTestUtils.setField(publisher, "exceptionTopic", "exception-queue");
        ReflectionTestUtils.setField(publisher, "pacs002RequestsTopic", "pacs002-requests");

        @SuppressWarnings("unchecked")
        CompletableFuture<SendResult<String, GenericRecord>> fut = (CompletableFuture<SendResult<String, GenericRecord>>) (CompletableFuture<?>) CompletableFuture.completedFuture(mock(SendResult.class));
        when(avroTemplate.send(anyString(), anyString(), any())).thenReturn(fut);
    }

    @Test
    void publishValidUnifiedSendsAvro() {
        Schema schema = publisher.getUnifiedSchema();
        GenericRecord rec = new GenericData.Record(schema);
        rec.put("messageType", org.apache.avro.generic.GenericData.get().createEnum("PACS_003", schema.getField("messageType").schema()));
        rec.put("messageVersion", "11");
        rec.put("messageId", "K1");
        rec.put("creationDateTime", java.time.Instant.now().toString());
        rec.put("supplementaryData", new java.util.HashMap<String, String>() {{ put("rawUnifiedJson", "{}"); }});
        publisher.publishValidUnified(rec, "K1");
        verify(avroTemplate).send(eq("payment-messages"), eq("K1"), any(GenericRecord.class));
    }

    @Test
    void publishInvalidSendsExceptionAvro() {
        publisher.publishInvalid("PX", "<x/>");
        verify(avroTemplate).send(eq("exception-queue"), eq("PX"), any(GenericRecord.class));
    }

    @Test
    void publishPacs002RequestSendsAvro() {
        publisher.publishPacs002Request("P2", "{} ");
        verify(avroTemplate).send(eq("pacs002-requests"), eq("P2"), any(GenericRecord.class));
    }
}
