package com.anz.fastpayment.inward.scheme.validation.exception;

import java.time.LocalDateTime;

/**
 * Validation exception for banking operations
 * Provides structured error information for audit and compliance
 */
public class ValidationException extends RuntimeException {
    
    private final String errorCode;
    private final String field;
    private final LocalDateTime timestamp;
    private final String transactionId;
    
    public ValidationException(String message, String errorCode, String field, String transactionId) {
        super(message);
        this.errorCode = errorCode;
        this.field = field;
        this.timestamp = LocalDateTime.now();
        this.transactionId = transactionId;
    }
    
    public ValidationException(String message, String errorCode, String field, String transactionId, Throwable cause) {
        super(message, cause);
        this.errorCode = errorCode;
        this.field = field;
        this.timestamp = LocalDateTime.now();
        this.transactionId = transactionId;
    }
    
    public String getErrorCode() {
        return errorCode;
    }
    
    public String getField() {
        return field;
    }
    
    public LocalDateTime getTimestamp() {
        return timestamp;
    }
    
    public String getTransactionId() {
        return transactionId;
    }
    
    @Override
    public String toString() {
        return "ValidationException{" +
                "errorCode='" + errorCode + '\'' +
                ", field='" + field + '\'' +
                ", timestamp=" + timestamp +
                ", transactionId='" + transactionId + '\'' +
                ", message='" + getMessage() + '\'' +
                '}';
    }
}
