package com.anz.fastpayment.inward.scheme.validation.model;

/**
 * Validation Status Enum for Banking Operations
 * Represents different states of validation
 */
public enum ValidationStatus {
    
    /**
     * Validation passed successfully
     */
    SUCCESS("SUCCESS"),
    
    /**
     * Validation failed
     */
    FAILURE("FAILURE"),
    
    /**
     * Tag is missing from the message
     */
    MISSING("MISSING");
    
    private final String value;
    
    ValidationStatus(String value) {
        this.value = value;
    }
    
    public String getValue() {
        return value;
    }
    
    @Override
    public String toString() {
        return value;
    }
}
