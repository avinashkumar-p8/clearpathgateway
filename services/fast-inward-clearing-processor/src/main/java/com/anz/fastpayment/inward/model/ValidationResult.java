package com.anz.fastpayment.inward.model;

import java.util.List;
import java.util.ArrayList;
import java.util.Collections;

/**
 * Validation result for clearing requests
 * Uses Java 21 record for immutability and concise syntax
 */
public record ValidationResult(
    boolean isValid,
    List<ValidationError> errors,
    String transactionId,
    long validationTimestamp
) {
    
    /**
     * Canonical constructor with validation
     */
    public ValidationResult {
        if (errors == null) {
            errors = new ArrayList<>();
        }
        if (transactionId == null) {
            throw new IllegalArgumentException("Transaction ID cannot be null");
        }
        if (validationTimestamp <= 0) {
            validationTimestamp = System.currentTimeMillis();
        }
    }
    
    /**
     * Factory method for successful validation
     */
    public static ValidationResult success(String transactionId) {
        return new ValidationResult(true, List.of(), transactionId, System.currentTimeMillis());
    }
    
    /**
     * Factory method for failed validation
     */
    public static ValidationResult failure(String transactionId, List<ValidationError> errors) {
        return new ValidationResult(false, errors, transactionId, System.currentTimeMillis());
    }
    
    /**
     * Factory method for failed validation with single error
     */
    public static ValidationResult failure(String transactionId, ValidationError error) {
        return new ValidationResult(false, List.of(error), transactionId, System.currentTimeMillis());
    }
    
    /**
     * Add an error to the validation result
     */
    public ValidationResult withError(ValidationError error) {
        List<ValidationError> newErrors = new ArrayList<>(errors);
        newErrors.add(error);
        return new ValidationResult(isValid, newErrors, transactionId, validationTimestamp);
    }
    
    /**
     * Get errors as unmodifiable list
     */
    public List<ValidationError> getErrors() {
        return Collections.unmodifiableList(errors);
    }
    
    /**
     * Check if there are any errors
     */
    public boolean hasErrors() {
        return !errors.isEmpty();
    }
    
    /**
     * Get first error message
     */
    public String getFirstErrorMessage() {
        return errors.isEmpty() ? "No errors" : errors.get(0).message();
    }
}

