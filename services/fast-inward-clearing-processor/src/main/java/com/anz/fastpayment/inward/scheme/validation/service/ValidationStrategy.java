package com.anz.fastpayment.inward.scheme.validation.service;

import com.anz.fastpayment.inward.scheme.validation.exception.ValidationException;

/**
 * Validation Strategy Interface for Banking Operations
 * Implements Strategy pattern for different validation types
 */
public interface ValidationStrategy {
    
    /**
     * Validate the given value
     * 
     * @param value The value to validate
     * @param transactionId The transaction ID for logging
     * @throws ValidationException if validation fails
     */
    void validate(String value, String transactionId) throws ValidationException;
    
    /**
     * Get the validation type identifier
     * 
     * @return The validation type (e.g., "currency", "country", "mmbid")
     */
    String getValidationType();
}
