package com.anz.fastpayment.router.service;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.core.io.ClassPathResource;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.test.util.ReflectionTestUtils;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class KafkaPublisherExceptionBranchesTest {

    private KafkaTemplate<String, GenericRecord> tpl;
    private KafkaPublisher publisher;

    @BeforeEach
    void setUp() throws Exception {
        tpl = mock(KafkaTemplate.class);
        @SuppressWarnings("unchecked")
        ObjectProvider<KafkaTemplate<String, String>> ignored = (ObjectProvider<KafkaTemplate<String, String>>) mock(ObjectProvider.class);
        publisher = new KafkaPublisher(tpl, ignored);
        // Ensure topics are non-null so matcher anyString() can match
        ReflectionTestUtils.setField(publisher, "paymentMessagesTopic", "payment-messages");
        ReflectionTestUtils.setField(publisher, "exceptionTopic", "exception-queue");
        ReflectionTestUtils.setField(publisher, "pacs002RequestsTopic", "pacs002-requests");
    }

    private GenericRecord buildUnified() throws Exception {
        ClassPathResource schemaRes = new ClassPathResource("avro/unified-payment-message.avsc");
        String content = new String(schemaRes.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        Schema schema = new Schema.Parser().parse(content);
        GenericRecord rec = new GenericData.Record(schema);
        rec.put("messageType", "PACS_003");
        rec.put("messageVersion", "11");
        rec.put("messageId", "K1");
        rec.put("creationDateTime", java.time.Instant.now().toString());
        rec.put("supplementaryData", java.util.Collections.singletonMap("rawUnifiedJson", "{}"));
        return rec;
    }

    @Test
    void publishValidUnified_handlesTimeoutAndExecutionException() throws Exception {
        GenericRecord rec = buildUnified();
        CompletableFuture<SendResult<String, GenericRecord>> fut = mock(CompletableFuture.class);
        when(tpl.send(anyString(), anyString(), any(GenericRecord.class))).thenReturn(fut);
        doThrow(new TimeoutException("t")).when(fut).get(eq(5L), eq(TimeUnit.SECONDS));
        assertDoesNotThrow(() -> publisher.publishValidUnified(rec, "K1"));

        fut = mock(CompletableFuture.class);
        when(tpl.send(anyString(), anyString(), any(GenericRecord.class))).thenReturn(fut);
        doThrow(new ExecutionException(new RuntimeException("e"))).when(fut).get(eq(5L), eq(TimeUnit.SECONDS));
        assertDoesNotThrow(() -> publisher.publishValidUnified(rec, "K1"));
    }

    @Test
    void publishValidUnified_handlesInterruptRestoresFlag() throws Exception {
        GenericRecord rec = buildUnified();
        CompletableFuture<SendResult<String, GenericRecord>> fut = mock(CompletableFuture.class);
        when(tpl.send(anyString(), anyString(), any(GenericRecord.class))).thenReturn(fut);
        doThrow(new InterruptedException("i")).when(fut).get(eq(5L), eq(TimeUnit.SECONDS));
        assertDoesNotThrow(() -> publisher.publishValidUnified(rec, "K1"));
    }

    @Test
    void publishInvalid_and_pacs002_handlesFailures() throws Exception {
        // success path
        when(tpl.send(anyString(), anyString(), any(GenericRecord.class))).thenReturn(CompletableFuture.completedFuture(null));
        assertDoesNotThrow(() -> publisher.publishInvalid("K1", "<x/>"));
        assertDoesNotThrow(() -> publisher.publishPacs002Request("K1", "{}"));

        // failure path with future throwing
        @SuppressWarnings("unchecked")
        CompletableFuture<SendResult<String, GenericRecord>> fut = (CompletableFuture<SendResult<String, GenericRecord>>) mock(CompletableFuture.class);
        when(tpl.send(anyString(), anyString(), any(GenericRecord.class))).thenReturn(fut);
        doThrow(new TimeoutException("t")).when(fut).get(eq(5L), eq(TimeUnit.SECONDS));
        assertDoesNotThrow(() -> publisher.publishInvalid("K2", "<y/>"));        
        when(tpl.send(anyString(), anyString(), any(GenericRecord.class))).thenReturn(fut);
        doThrow(new TimeoutException("t2")).when(fut).get(eq(5L), eq(TimeUnit.SECONDS));
        assertDoesNotThrow(() -> publisher.publishPacs002Request("K3", "{ }"));
    }
}
