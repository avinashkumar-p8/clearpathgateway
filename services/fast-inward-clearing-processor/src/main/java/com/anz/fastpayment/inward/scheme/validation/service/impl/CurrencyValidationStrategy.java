package com.anz.fastpayment.inward.scheme.validation.service.impl;

import com.anz.fastpayment.inward.scheme.validation.exception.ValidationException;
import com.anz.fastpayment.inward.scheme.validation.service.ValidationStrategy;
import com.anz.fastpayment.inward.scheme.validation.service.CurrencyCountryValidationService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Currency Validation Strategy Implementation
 * Reuses existing CurrencyCountryValidationService for currency validation
 */
@Component
public class CurrencyValidationStrategy implements ValidationStrategy {
    
    private static final Logger log = LoggerFactory.getLogger(CurrencyValidationStrategy.class);
    
    private final CurrencyCountryValidationService currencyCountryValidationService;
    
    @Autowired
    public CurrencyValidationStrategy(CurrencyCountryValidationService currencyCountryValidationService) {
        this.currencyCountryValidationService = currencyCountryValidationService;
    }
    
    @Override
    public void validate(String value, String transactionId) throws ValidationException {
        log.debug("Validating currency: {} for transaction: {}", value, transactionId);
        
        if (value == null || value.trim().isEmpty()) {
            throw new ValidationException(
                "Currency cannot be null or empty",
                "INVALID_CURRENCY",
                "currency",
                transactionId
            );
        }
        
        String currencyCode = value.trim();
        
        // Use existing validation service to check if currency exists
        var currency = currencyCountryValidationService.getCurrencyByCode(currencyCode);
        if (currency.isEmpty()) {
            throw new ValidationException(
                "Invalid currency code: " + currencyCode,
                "INVALID_CURRENCY",
                "currency",
                transactionId
            );
        }
        
        log.debug("Currency validation successful: {} for transaction: {}", value, transactionId);
    }
    
    @Override
    public String getValidationType() {
        return "currency";
    }
}
