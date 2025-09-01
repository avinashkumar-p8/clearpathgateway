package com.anz.fastpayment.inward.handler;

import com.anz.fastpayment.inward.scheme.validation.model.ValidationResult;

import java.util.Map;

/**
 * Handler for message validation operations
 * Uses existing SchemeValidationOrchestrator for validation
 */
public interface ValidationHandler {
    
    /**
     * Validate message payload
     * 
     * @param messagePayload Message payload as Map<String, Object>
     * @return ValidationResult containing validation outcome
     */
    ValidationResult validate(Map<String, Object> messagePayload);
    
    /**
     * Validate message payload with transaction ID
     * 
     * @param messagePayload Message payload as Map<String, Object>
     * @param transactionId Transaction identifier for logging
     * @return ValidationResult containing validation outcome
     */
    ValidationResult validate(Map<String, Object> messagePayload, String transactionId);
    
    /**
     * Check if validation result indicates success
     * 
     * @param validationResult Validation result to check
     * @return true if validation was successful
     */
    boolean isValidationSuccessful(ValidationResult validationResult);
    
    /**
     * Get validation errors from validation result
     * 
     * @param validationResult Validation result
     * @return List of validation error messages
     */
    java.util.List<String> getValidationErrors(ValidationResult validationResult);
}
