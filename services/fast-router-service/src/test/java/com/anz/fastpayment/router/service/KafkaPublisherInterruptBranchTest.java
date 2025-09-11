package com.anz.fastpayment.router.service;

import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class KafkaPublisherInterruptBranchTest {

    private KafkaTemplate<String, GenericRecord> tpl;
    private KafkaPublisher publisher;

    @BeforeEach
    void setUp() {
        tpl = mock(KafkaTemplate.class);
        @SuppressWarnings("unchecked")
        ObjectProvider<KafkaTemplate<String, String>> ignored = (ObjectProvider<KafkaTemplate<String, String>>) mock(ObjectProvider.class);
        publisher = new KafkaPublisher(tpl, ignored);
        ReflectionTestUtils.setField(publisher, "paymentMessagesTopic", "payment-messages");
        ReflectionTestUtils.setField(publisher, "exceptionTopic", "exception-queue");
        ReflectionTestUtils.setField(publisher, "pacs002RequestsTopic", "pacs002-requests");
    }

    @Test
    void interruptBranchesAreCaughtForInvalidAndPacs002() throws Exception {
        @SuppressWarnings("unchecked")
        CompletableFuture<SendResult<String, GenericRecord>> fut = (CompletableFuture<SendResult<String, GenericRecord>>) mock(CompletableFuture.class);
        when(tpl.send(anyString(), anyString(), any(GenericRecord.class))).thenReturn(fut);
        doThrow(new InterruptedException("i1")).when(fut).get(eq(5L), eq(TimeUnit.SECONDS));
        assertDoesNotThrow(() -> publisher.publishInvalid("K1", "<x/>"));

        when(tpl.send(anyString(), anyString(), any(GenericRecord.class))).thenReturn(fut);
        doThrow(new InterruptedException("i2")).when(fut).get(eq(5L), eq(TimeUnit.SECONDS));
        assertDoesNotThrow(() -> publisher.publishPacs002Request("K1", "{}"));
    }
}
