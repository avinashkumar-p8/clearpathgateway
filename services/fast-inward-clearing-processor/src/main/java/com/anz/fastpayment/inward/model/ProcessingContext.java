package com.anz.fastpayment.inward.model;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.time.LocalDateTime;

/**
 * Processing Context for transaction processing pipeline
 * Contains all necessary information for processing a transaction message
 */
public class ProcessingContext {
    
    private final ConsumerRecord<String, GenericRecord> consumerRecord;
    private final String topic;
    private final Integer partition;
    private final Long offset;
    private final GenericRecord avroMessage;
    private final String transactionId;
    private final LocalDateTime processingStartTime;
    
    public ProcessingContext(ConsumerRecord<String, GenericRecord> consumerRecord) {
        this.consumerRecord = consumerRecord;
        this.topic = consumerRecord.topic();
        this.partition = consumerRecord.partition();
        this.offset = consumerRecord.offset();
        this.avroMessage = consumerRecord.value();
        this.transactionId = consumerRecord.key();
        this.processingStartTime = LocalDateTime.now();
    }
    
    // Getters
    public ConsumerRecord<String, GenericRecord> getConsumerRecord() {
        return consumerRecord;
    }
    
    public String getTopic() {
        return topic;
    }
    
    public Integer getPartition() {
        return partition;
    }
    
    public Long getOffset() {
        return offset;
    }
    
    public GenericRecord getAvroMessage() {
        return avroMessage;
    }
    
    public String getTransactionId() {
        return transactionId;
    }
    
    public LocalDateTime getProcessingStartTime() {
        return processingStartTime;
    }
    
    /**
     * Get payload as string for idempotency service
     */
    public String getPayloadAsString() {
        return avroMessage != null ? avroMessage.toString() : "null";
    }
    
    /**
     * Get processing duration in milliseconds
     */
    public long getProcessingDurationMs() {
        return java.time.Duration.between(processingStartTime, LocalDateTime.now()).toMillis();
    }
    
    @Override
    public String toString() {
        return String.format("ProcessingContext{topic='%s', partition=%d, offset=%d, transactionId='%s', processingStartTime=%s}", 
                           topic, partition, offset, transactionId, processingStartTime);
    }
}
