package com.anz.fastpayment.inward.scheme.validation.service.impl;

import com.anz.fastpayment.inward.scheme.validation.exception.ValidationException;
import com.anz.fastpayment.inward.scheme.validation.service.ValidationStrategy;
import com.anz.fastpayment.inward.scheme.validation.service.CurrencyCountryValidationService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Country Validation Strategy Implementation
 * Reuses existing CurrencyCountryValidationService for country validation
 */
@Component
public class CountryValidationStrategy implements ValidationStrategy {
    
    private static final Logger log = LoggerFactory.getLogger(CountryValidationStrategy.class);
    
    private final CurrencyCountryValidationService currencyCountryValidationService;
    
    @Autowired
    public CountryValidationStrategy(CurrencyCountryValidationService currencyCountryValidationService) {
        this.currencyCountryValidationService = currencyCountryValidationService;
    }
    
    @Override
    public void validate(String value, String transactionId) throws ValidationException {
        log.debug("Validating country: {} for transaction: {}", value, transactionId);
        
        if (value == null || value.trim().isEmpty()) {
            throw new ValidationException(
                "Country cannot be null or empty",
                "INVALID_COUNTRY",
                "country",
                transactionId
            );
        }
        
        String countryCode = value.trim();
        
        // Use existing validation service to check if country exists
        var country = currencyCountryValidationService.getCountryByCode(countryCode);
        if (country.isEmpty()) {
            throw new ValidationException(
                "Invalid country code: " + countryCode,
                "INVALID_COUNTRY",
                "country",
                transactionId
            );
        }
        
        log.debug("Country validation successful: {} for transaction: {}", value, transactionId);
    }
    
    @Override
    public String getValidationType() {
        return "country";
    }
}
