package com.anz.fastpayment.inward.scheme.validation.exception;

import java.time.LocalDateTime;

/**
 * Idempotency Exception for Banking Operations
 * Handles errors related to message idempotency processing
 * Provides structured error information for audit and compliance
 */
public class IdempotencyException extends RuntimeException {
    
    private final String errorCode;
    private final String muid;
    private final String transactionId;
    private final LocalDateTime timestamp;
    private final String operation;
    
    public IdempotencyException(String message, String errorCode, String muid, String transactionId, String operation) {
        super(message);
        this.errorCode = errorCode;
        this.muid = muid;
        this.transactionId = transactionId;
        this.timestamp = LocalDateTime.now();
        this.operation = operation;
    }
    
    public IdempotencyException(String message, String errorCode, String muid, String transactionId, String operation, Throwable cause) {
        super(message, cause);
        this.errorCode = errorCode;
        this.muid = muid;
        this.transactionId = transactionId;
        this.timestamp = LocalDateTime.now();
        this.operation = operation;
    }
    
    public String getErrorCode() {
        return errorCode;
    }
    
    public String getMuid() {
        return muid;
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
        return "IdempotencyException{" +
                "errorCode='" + errorCode + '\'' +
                ", muid='" + muid + '\'' +
                ", transactionId='" + transactionId + '\'' +
                ", timestamp=" + timestamp +
                ", operation='" + operation + '\'' +
                ", message='" + getMessage() + '\'' +
                '}';
    }
}
