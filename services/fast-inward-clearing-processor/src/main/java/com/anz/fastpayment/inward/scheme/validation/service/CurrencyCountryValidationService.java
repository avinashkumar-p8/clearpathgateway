package com.anz.fastpayment.inward.scheme.validation.service;

import com.anz.fastpayment.inward.scheme.validation.entity.Currency;
import com.anz.fastpayment.inward.scheme.validation.entity.Country;
import com.anz.fastpayment.inward.scheme.validation.repository.CurrencyRepository;
import com.anz.fastpayment.inward.scheme.validation.repository.CountryRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Optional;

/**
 * Service for validating currency and country codes against database
 * Provides business logic for currency and country validation
 */
@Service
public class CurrencyCountryValidationService {
    
    private static final Logger log = LoggerFactory.getLogger(CurrencyCountryValidationService.class);
    
    @Autowired
    private CurrencyRepository currencyRepository;
    
    @Autowired
    private CountryRepository countryRepository;
    
    /**
     * Validate currency code against database
     * @param currencyCode The currency code to validate
     * @return Validation result with details
     */
    public CurrencyValidationResult validateCurrency(String currencyCode) {
        log.debug("Validating currency code: {}", currencyCode);
        
        if (currencyCode == null || currencyCode.trim().isEmpty()) {
            return CurrencyValidationResult.invalid("Currency code cannot be null or empty");
        }
        
        String trimmedCode = currencyCode.trim().toUpperCase();
        
        // Check if currency exists in database
        Optional<Currency> currencyOpt = currencyRepository.findByCode(trimmedCode);
        if (currencyOpt.isEmpty()) {
            log.warn("Currency code not found in database: {}", trimmedCode);
            return CurrencyValidationResult.invalid("Currency code not found: " + trimmedCode);
        }
        
        Currency currency = currencyOpt.get();
        
        // Check if currency is active
        if (!currency.isActive()) {
            log.warn("Currency code is inactive: {}", trimmedCode);
            return CurrencyValidationResult.invalid("Currency code is inactive: " + trimmedCode);
        }
        
        log.debug("Currency validation successful: {}", trimmedCode);
        return CurrencyValidationResult.valid(currency);
    }
    
    /**
     * Validate country code against database
     * @param countryCode The country code to validate
     * @return Validation result with details
     */
    public CountryValidationResult validateCountry(String countryCode) {
        log.debug("Validating country code: {}", countryCode);
        
        if (countryCode == null || countryCode.trim().isEmpty()) {
            return CountryValidationResult.invalid("Country code cannot be null or empty");
        }
        
        String trimmedCode = countryCode.trim().toUpperCase();
        
        // Check if country exists in database
        Optional<Country> countryOpt = countryRepository.findByCode(trimmedCode);
        if (countryOpt.isEmpty()) {
            log.warn("Country code not found in database: {}", trimmedCode);
            return CountryValidationResult.invalid("Country code not found: " + trimmedCode);
        }
        
        Country country = countryOpt.get();
        
        // Check if country is active
        if (!country.isActive()) {
            log.warn("Country code is inactive: {}", trimmedCode);
            return CountryValidationResult.invalid("Country code is inactive: " + trimmedCode);
        }
        
        log.debug("Country validation successful: {}", trimmedCode);
        return CountryValidationResult.valid(country);
    }
    
    /**
     * Validate currency-country combination
     * @param currencyCode The currency code
     * @param countryCode The country code
     * @return Validation result
     */
    public CurrencyCountryValidationResult validateCurrencyCountry(String currencyCode, String countryCode) {
        log.debug("Validating currency-country combination: {} - {}", currencyCode, countryCode);
        
        // Validate currency first
        CurrencyValidationResult currencyResult = validateCurrency(currencyCode);
        if (!currencyResult.isValid()) {
            return CurrencyCountryValidationResult.invalid(currencyResult.getErrorMessage());
        }
        
        // Validate country
        CountryValidationResult countryResult = validateCountry(countryCode);
        if (!countryResult.isValid()) {
            return CurrencyCountryValidationResult.invalid(countryResult.getErrorMessage());
        }
        
        // Check if currency is valid for the country
        Currency currency = currencyResult.getCurrency();
        Country country = countryResult.getCountry();
        
        if (currency.getValidCountries() != null && 
            !currency.getValidCountries().contains(countryCode.toUpperCase())) {
            log.warn("Currency {} is not valid for country {}", currencyCode, countryCode);
            return CurrencyCountryValidationResult.invalid(
                "Currency " + currencyCode + " is not valid for country " + countryCode);
        }
        
        log.debug("Currency-country validation successful: {} - {}", currencyCode, countryCode);
        return CurrencyCountryValidationResult.valid(currency, country);
    }
    
    /**
     * Get all active currencies
     * @return List of active currencies
     */
    public List<Currency> getAllActiveCurrencies() {
        return currencyRepository.findByActiveTrue();
    }
    
    /**
     * Get all active countries
     * @return List of active countries
     */
    public List<Country> getAllActiveCountries() {
        return countryRepository.findByActiveTrue();
    }
    
    /**
     * Check if currency exists
     * @param currencyCode The currency code to check
     * @return true if currency exists, false otherwise
     */
    public boolean currencyExists(String currencyCode) {
        return currencyRepository.existsByCode(currencyCode.toUpperCase());
    }
    
    /**
     * Check if country exists
     * @param countryCode The country code to check
     * @return true if country exists, false otherwise
     */
    public boolean countryExists(String countryCode) {
        return countryRepository.existsByCode(countryCode.toUpperCase());
    }
    
    // Inner classes for validation results
    public static class CurrencyValidationResult {
        private final boolean valid;
        private final Currency currency;
        private final String errorMessage;
        
        private CurrencyValidationResult(boolean valid, Currency currency, String errorMessage) {
            this.valid = valid;
            this.currency = currency;
            this.errorMessage = errorMessage;
        }
        
        public static CurrencyValidationResult valid(Currency currency) {
            return new CurrencyValidationResult(true, currency, null);
        }
        
        public static CurrencyValidationResult invalid(String errorMessage) {
            return new CurrencyValidationResult(false, null, errorMessage);
        }
        
        public boolean isValid() { return valid; }
        public Currency getCurrency() { return currency; }
        public String getErrorMessage() { return errorMessage; }
    }
    
    public static class CountryValidationResult {
        private final boolean valid;
        private final Country country;
        private final String errorMessage;
        
        private CountryValidationResult(boolean valid, Country country, String errorMessage) {
            this.valid = valid;
            this.country = country;
            this.errorMessage = errorMessage;
        }
        
        public static CountryValidationResult valid(Country country) {
            return new CountryValidationResult(true, country, null);
        }
        
        public static CountryValidationResult invalid(String errorMessage) {
            return new CountryValidationResult(false, null, errorMessage);
        }
        
        public boolean isValid() { return valid; }
        public Country getCountry() { return country; }
        public String getErrorMessage() { return errorMessage; }
    }
    
    public static class CurrencyCountryValidationResult {
        private final boolean valid;
        private final Currency currency;
        private final Country country;
        private final String errorMessage;
        
        private CurrencyCountryValidationResult(boolean valid, Currency currency, Country country, String errorMessage) {
            this.valid = valid;
            this.currency = currency;
            this.country = country;
            this.errorMessage = errorMessage;
        }
        
        public static CurrencyCountryValidationResult valid(Currency currency, Country country) {
            return new CurrencyCountryValidationResult(true, currency, country, null);
        }
        
        public static CurrencyCountryValidationResult invalid(String errorMessage) {
            return new CurrencyCountryValidationResult(false, null, null, errorMessage);
        }
        
        public boolean isValid() { return valid; }
        public Currency getCurrency() { return currency; }
        public Country getCountry() { return country; }
        public String getErrorMessage() { return errorMessage; }
    }
}
