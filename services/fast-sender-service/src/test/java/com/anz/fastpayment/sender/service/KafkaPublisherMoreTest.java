package com.anz.fastpayment.sender.service;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.concurrent.CompletableFuture;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

class KafkaPublisherMoreTest {
    @Test
    void success_withoutRecordMetadata_branch() {
        KafkaTemplate<String, GenericRecord> kt = mock(KafkaTemplate.class);
        CompletableFuture<org.springframework.kafka.support.SendResult<String, GenericRecord>> fut = new CompletableFuture<>();
        // result with null metadata
        fut.complete(new org.springframework.kafka.support.SendResult<>(new org.apache.kafka.clients.producer.ProducerRecord<>("t", null), null));
        when(kt.send(eq("t"), any(GenericRecord.class))).thenReturn(fut);
        KafkaPublisher p = new KafkaPublisher(kt);
        Schema s = new Schema.Parser().parse("{\n \"type\":\"record\",\n \"name\":\"E\",\n \"fields\":[{\"name\":\"a\",\"type\":\"string\"}]\n}");
        GenericRecord rec = new GenericData.Record(s);
        rec.put("a", "b");
        p.publish("t", rec);
        verify(kt, times(1)).send(eq("t"), any(GenericRecord.class));
    }
}


