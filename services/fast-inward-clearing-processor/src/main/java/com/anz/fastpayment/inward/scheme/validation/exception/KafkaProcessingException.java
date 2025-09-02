package com.anz.fastpayment.inward.scheme.validation.exception;

import java.time.LocalDateTime;

/**
 * Kafka Processing Exception for Banking Operations
 * Handles errors related to Kafka message processing, serialization, and transmission
 * Provides structured error information for audit and compliance
 */
public class KafkaProcessingException extends RuntimeException {
    
    private final String errorCode;
    private final String topic;
    private final String partition;
    private final String offset;
    private final String transactionId;
    private final LocalDateTime timestamp;
    private final String operation;
    
    public KafkaProcessingException(String message, String errorCode, String topic, String partition, 
                                   String offset, String transactionId, String operation) {
        super(message);
        this.errorCode = errorCode;
        this.topic = topic;
        this.partition = partition;
        this.offset = offset;
        this.transactionId = transactionId;
        this.timestamp = LocalDateTime.now();
        this.operation = operation;
    }
    
    public KafkaProcessingException(String message, String errorCode, String topic, String partition, 
                                   String offset, String transactionId, String operation, Throwable cause) {
        super(message, cause);
        this.errorCode = errorCode;
        this.topic = topic;
        this.partition = partition;
        this.offset = offset;
        this.transactionId = transactionId;
        this.timestamp = LocalDateTime.now();
        this.operation = operation;
    }
    
    public String getErrorCode() {
        return errorCode;
    }
    
    public String getTopic() {
        return topic;
    }
    
    public String getPartition() {
        return partition;
    }
    
    public String getOffset() {
        return offset;
    }
    
    public String getTransactionId() {
        return transactionId;
    }
    
    public LocalDateTime getTimestamp() {
        return timestamp;
    }
    
    public String getOperation() {
        return operation;
    }
    
    @Override
    public String toString() {
        return "KafkaProcessingException{" +
                "errorCode='" + errorCode + '\'' +
                ", topic='" + topic + '\'' +
                ", partition='" + partition + '\'' +
                ", offset='" + offset + '\'' +
                ", transactionId='" + transactionId + '\'' +
                ", timestamp=" + timestamp +
                ", operation='" + operation + '\'' +
                ", message='" + getMessage() + '\'' +
                '}';
    }
}
