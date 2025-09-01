package com.anz.fastpayment.inward.controller;

import com.anz.fastpayment.inward.scheme.validation.exception.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.context.request.WebRequest;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;

/**
 * Global Exception Handler for Banking Operations
 * Provides centralized exception handling for all REST endpoints
 * Ensures consistent error responses and proper logging
 */
@ControllerAdvice
public class GlobalExceptionHandler {
    
    private static final Logger logger = LoggerFactory.getLogger(GlobalExceptionHandler.class);
    
    /**
     * Handle Validation Exceptions
     */
    @ExceptionHandler(ValidationException.class)
    public ResponseEntity<Map<String, Object>> handleValidationException(ValidationException ex, WebRequest request) {
        logger.warn("Validation error: {} - {}", ex.getErrorCode(), ex.getMessage());
        
        Map<String, Object> errorResponse = createErrorResponse(
            "VALIDATION_ERROR",
            ex.getMessage(),
            ex.getErrorCode(),
            ex.getField(),
            ex.getTransactionId(),
            ex.getTimestamp()
        );
        
        return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(errorResponse);
    }
    
    /**
     * Handle Idempotency Exceptions
     */
    @ExceptionHandler(IdempotencyException.class)
    public ResponseEntity<Map<String, Object>> handleIdempotencyException(IdempotencyException ex, WebRequest request) {
        logger.error("Idempotency error: {} - {} for MUID: {}", ex.getErrorCode(), ex.getMessage(), ex.getMuid());
        
        Map<String, Object> errorResponse = createErrorResponse(
            "IDEMPOTENCY_ERROR",
            ex.getMessage(),
            ex.getErrorCode(),
            "muid",
            ex.getTransactionId(),
            ex.getTimestamp()
        );
        
        errorResponse.put("muid", ex.getMuid());
        errorResponse.put("operation", ex.getOperation());
        
        return ResponseEntity.status(HttpStatus.CONFLICT).body(errorResponse);
    }
    
    /**
     * Handle Kafka Processing Exceptions
     */
    @ExceptionHandler(KafkaProcessingException.class)
    public ResponseEntity<Map<String, Object>> handleKafkaProcessingException(KafkaProcessingException ex, WebRequest request) {
        logger.error("Kafka processing error: {} - {} for topic: {} partition: {} offset: {}", 
            ex.getErrorCode(), ex.getMessage(), ex.getTopic(), ex.getPartition(), ex.getOffset());
        
        Map<String, Object> errorResponse = createErrorResponse(
            "KAFKA_PROCESSING_ERROR",
            ex.getMessage(),
            ex.getErrorCode(),
            "kafka",
            ex.getTransactionId(),
            ex.getTimestamp()
        );
        
        errorResponse.put("topic", ex.getTopic());
        errorResponse.put("partition", ex.getPartition());
        errorResponse.put("offset", ex.getOffset());
        errorResponse.put("operation", ex.getOperation());
        
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(errorResponse);
    }
    
    /**
     * Handle Database Operation Exceptions
     */
    @ExceptionHandler(DatabaseOperationException.class)
    public ResponseEntity<Map<String, Object>> handleDatabaseOperationException(DatabaseOperationException ex, WebRequest request) {
        logger.error("Database operation error: {} - {} for entity: {} operation: {}", 
            ex.getErrorCode(), ex.getMessage(), ex.getEntity(), ex.getOperation());
        
        Map<String, Object> errorResponse = createErrorResponse(
            "DATABASE_OPERATION_ERROR",
            ex.getMessage(),
            ex.getErrorCode(),
            ex.getEntity(),
            ex.getIdentifier(),
            ex.getTimestamp()
        );
        
        errorResponse.put("operation", ex.getOperation());
        errorResponse.put("sqlState", ex.getSqlState());
        errorResponse.put("sqlErrorCode", ex.getSqlErrorCode());
        
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(errorResponse);
    }
    
    /**
     * Handle Cache Operation Exceptions
     */
    @ExceptionHandler(CacheOperationException.class)
    public ResponseEntity<Map<String, Object>> handleCacheOperationException(CacheOperationException ex, WebRequest request) {
        logger.warn("Cache operation error: {} - {} for cache: {} operation: {}", 
            ex.getErrorCode(), ex.getMessage(), ex.getCacheName(), ex.getOperation());
        
        Map<String, Object> errorResponse = createErrorResponse(
            "CACHE_OPERATION_ERROR",
            ex.getMessage(),
            ex.getErrorCode(),
            ex.getCacheName(),
            ex.getKey(),
            ex.getTimestamp()
        );
        
        errorResponse.put("operation", ex.getOperation());
        errorResponse.put("cacheType", ex.getCacheType());
        
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(errorResponse);
    }
    
    /**
     * Handle Generic Runtime Exceptions
     */
    @ExceptionHandler(RuntimeException.class)
    public ResponseEntity<Map<String, Object>> handleRuntimeException(RuntimeException ex, WebRequest request) {
        logger.error("Runtime error: {}", ex.getMessage(), ex);
        
        Map<String, Object> errorResponse = createErrorResponse(
            "RUNTIME_ERROR",
            "An unexpected error occurred",
            "UNKNOWN_ERROR",
            "system",
            "UNKNOWN",
            LocalDateTime.now()
        );
        
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(errorResponse);
    }
    
    /**
     * Handle Generic Exceptions
     */
    @ExceptionHandler(Exception.class)
    public ResponseEntity<Map<String, Object>> handleGenericException(Exception ex, WebRequest request) {
        logger.error("Generic error: {}", ex.getMessage(), ex);
        
        Map<String, Object> errorResponse = createErrorResponse(
            "GENERIC_ERROR",
            "An unexpected error occurred",
            "UNKNOWN_ERROR",
            "system",
            "UNKNOWN",
            LocalDateTime.now()
        );
        
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(errorResponse);
    }
    
    /**
     * Create standardized error response
     */
    private Map<String, Object> createErrorResponse(String type, String message, String errorCode, 
                                                   String field, String identifier, LocalDateTime timestamp) {
        Map<String, Object> errorResponse = new HashMap<>();
        errorResponse.put("type", type);
        errorResponse.put("message", message);
        errorResponse.put("errorCode", errorCode);
        errorResponse.put("field", field);
        errorResponse.put("identifier", identifier);
        errorResponse.put("timestamp", timestamp);
        errorResponse.put("path", "/api/v1/error"); // This will be updated by actual request path
        
        return errorResponse;
    }
}
