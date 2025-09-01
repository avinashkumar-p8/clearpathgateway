package com.anz.fastpayment.inward.scheme.validation.exception;

import java.time.LocalDateTime;

/**
 * Cache Operation Exception for Banking Operations
 * Handles errors related to cache operations, eviction, and data retrieval
 * Provides structured error information for audit and compliance
 */
public class CacheOperationException extends RuntimeException {
    
    private final String errorCode;
    private final String cacheName;
    private final String operation;
    private final String key;
    private final LocalDateTime timestamp;
    private final String cacheType;
    
    public CacheOperationException(String message, String errorCode, String cacheName, 
                                  String operation, String key, String cacheType) {
        super(message);
        this.errorCode = errorCode;
        this.cacheName = cacheName;
        this.operation = operation;
        this.key = key;
        this.timestamp = LocalDateTime.now();
        this.cacheType = cacheType;
    }
    
    public CacheOperationException(String message, String errorCode, String cacheName, 
                                  String operation, String key, String cacheType, Throwable cause) {
        super(message, cause);
        this.errorCode = errorCode;
        this.cacheName = cacheName;
        this.operation = operation;
        this.key = key;
        this.timestamp = LocalDateTime.now();
        this.cacheType = cacheType;
    }
    
    public String getErrorCode() {
        return errorCode;
    }
    
    public String getCacheName() {
        return cacheName;
    }
    
    public String getOperation() {
        return operation;
    }
    
    public String getKey() {
        return key;
    }
    
    public LocalDateTime getTimestamp() {
        return timestamp;
    }
    
    public String getCacheType() {
        return cacheType;
    }
    
    @Override
    public String toString() {
        return "CacheOperationException{" +
                "errorCode='" + errorCode + '\'' +
                ", cacheName='" + cacheName + '\'' +
                ", operation='" + operation + '\'' +
                ", key='" + key + '\'' +
                ", timestamp=" + timestamp +
                ", cacheType='" + cacheType + '\'' +
                ", message='" + getMessage() + '\'' +
                '}';
    }
}
