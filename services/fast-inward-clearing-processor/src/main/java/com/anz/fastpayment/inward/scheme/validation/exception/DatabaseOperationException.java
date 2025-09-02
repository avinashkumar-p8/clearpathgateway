package com.anz.fastpayment.inward.scheme.validation.exception;

import java.time.LocalDateTime;

/**
 * Database Operation Exception for Banking Operations
 * Handles errors related to database operations, transactions, and data persistence
 * Provides structured error information for audit and compliance
 */
public class DatabaseOperationException extends RuntimeException {
    
    private final String errorCode;
    private final String operation;
    private final String entity;
    private final String identifier;
    private final LocalDateTime timestamp;
    private final String sqlState;
    private final int sqlErrorCode;
    
    public DatabaseOperationException(String message, String errorCode, String operation, 
                                     String entity, String identifier, String sqlState, int sqlErrorCode) {
        super(message);
        this.errorCode = errorCode;
        this.operation = operation;
        this.entity = entity;
        this.identifier = identifier;
        this.timestamp = LocalDateTime.now();
        this.sqlState = sqlState;
        this.sqlErrorCode = sqlErrorCode;
    }
    
    public DatabaseOperationException(String message, String errorCode, String operation, 
                                     String entity, String identifier, String sqlState, int sqlErrorCode, Throwable cause) {
        super(message, cause);
        this.errorCode = errorCode;
        this.operation = operation;
        this.entity = entity;
        this.identifier = identifier;
        this.timestamp = LocalDateTime.now();
        this.sqlState = sqlState;
        this.sqlErrorCode = sqlErrorCode;
    }
    
    public String getErrorCode() {
        return errorCode;
    }
    
    public String getOperation() {
        return operation;
    }
    
    public String getEntity() {
        return entity;
    }
    
    public String getIdentifier() {
        return identifier;
    }
    
    public LocalDateTime getTimestamp() {
        return timestamp;
    }
    
    public String getSqlState() {
        return sqlState;
    }
    
    public int getSqlErrorCode() {
        return sqlErrorCode;
    }
    
    @Override
    public String toString() {
        return "DatabaseOperationException{" +
                "errorCode='" + errorCode + '\'' +
                ", operation='" + operation + '\'' +
                ", entity='" + entity + '\'' +
                ", identifier='" + identifier + '\'' +
                ", timestamp=" + timestamp +
                ", sqlState='" + sqlState + '\'' +
                ", sqlErrorCode=" + sqlErrorCode +
                ", message='" + getMessage() + '\'' +
                '}';
    }
}
