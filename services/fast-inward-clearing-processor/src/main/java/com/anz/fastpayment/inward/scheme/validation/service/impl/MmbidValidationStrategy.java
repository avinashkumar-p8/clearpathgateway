package com.anz.fastpayment.inward.scheme.validation.service.impl;

import com.anz.fastpayment.inward.scheme.validation.exception.ValidationException;
import com.anz.fastpayment.inward.scheme.validation.service.ValidationStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * MMBID Validation Strategy Implementation
 * Validates that MMBID has exactly 11 characters
 */
@Component
public class MmbidValidationStrategy implements ValidationStrategy {
    
    private static final Logger log = LoggerFactory.getLogger(MmbidValidationStrategy.class);
    private static final int REQUIRED_MMBID_LENGTH = 11;
    
    @Override
    public void validate(String value, String transactionId) throws ValidationException {
        log.debug("Validating MMBID: {} for transaction: {}", value, transactionId);
        
        if (value == null || value.trim().isEmpty()) {
            throw new ValidationException(
                "MMBID cannot be null or empty",
                "INVALID_MMBID",
                "mmbid",
                transactionId
            );
        }
        
        String trimmedValue = value.trim();
        if (trimmedValue.length() != REQUIRED_MMBID_LENGTH) {
            throw new ValidationException(
                String.format("MMBID must have exactly %d characters, found: %d", 
                    REQUIRED_MMBID_LENGTH, trimmedValue.length()),
                "INVALID_MMBID_LENGTH",
                "mmbid",
                transactionId
            );
        }
        
        log.debug("MMBID validation successful: {} for transaction: {}", value, transactionId);
    }
    
    @Override
    public String getValidationType() {
        return "mmbid";
    }
}
