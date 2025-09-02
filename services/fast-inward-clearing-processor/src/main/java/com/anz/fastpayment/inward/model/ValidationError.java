package com.anz.fastpayment.inward.model;

import java.time.LocalDateTime;

/**
 * Validation error details
 * Uses Java 21 record for immutability and concise syntax
 */
public record ValidationError(
    String field,
    String message,
    String code,
    LocalDateTime timestamp,
    ErrorSeverity severity
) {
    
    /**
     * Canonical constructor with validation
     */
    public ValidationError {
        if (field == null || field.isBlank()) {
            throw new IllegalArgumentException("Field name cannot be null or blank");
        }
        if (message == null || message.isBlank()) {
            throw new IllegalArgumentException("Error message cannot be null or blank");
        }
        if (code == null) {
            code = "VALIDATION_ERROR";
        }
        if (timestamp == null) {
            timestamp = LocalDateTime.now();
        }
        if (severity == null) {
            severity = ErrorSeverity.ERROR;
        }
    }
    
    /**
     * Factory method for field validation error
     */
    public static ValidationError fieldError(String field, String message) {
        return new ValidationError(field, message, "FIELD_VALIDATION_ERROR", LocalDateTime.now(), ErrorSeverity.ERROR);
    }
    
    /**
     * Factory method for business rule error
     */
    public static ValidationError businessRuleError(String field, String message) {
        return new ValidationError(field, message, "BUSINESS_RULE_ERROR", LocalDateTime.now(), ErrorSeverity.ERROR);
    }
    
    /**
     * Factory method for warning
     */
    public static ValidationError warning(String field, String message) {
        return new ValidationError(field, message, "VALIDATION_WARNING", LocalDateTime.now(), ErrorSeverity.WARNING);
    }
    
    /**
     * Error severity levels
     */
    public enum ErrorSeverity {
        ERROR, WARNING, INFO
    }
}

