package com.anz.fastpayment.inward.scheme.validation.service;

import com.anz.fastpayment.inward.scheme.validation.exception.ValidationException;
import com.anz.fastpayment.inward.scheme.validation.repository.CurrencyRepository;
import com.anz.fastpayment.inward.scheme.validation.repository.CountryRepository;
import com.anz.fastpayment.inward.scheme.validation.entity.Currency;
import com.anz.fastpayment.inward.scheme.validation.entity.Country;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.annotation.CacheEvict;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Optional;

/**
 * Currency and Country Validation Service for Banking Operations
 * Implements database-backed validation with caching for performance
 * Uses Java 21 features for modern, efficient validation
 */
@Service
@Transactional
public class CurrencyCountryValidationService {
    
    private static final Logger log = LoggerFactory.getLogger(CurrencyCountryValidationService.class);
    
    private final CurrencyRepository currencyRepository;
    private final CountryRepository countryRepository;
    
    @Autowired
    public CurrencyCountryValidationService(CurrencyRepository currencyRepository, 
                                          CountryRepository countryRepository) {
        this.currencyRepository = currencyRepository;
        this.countryRepository = countryRepository;
    }
    
    /**
     * Validate currency-country combination
     * Throws ValidationException for invalid combinations
     */
    @Transactional(readOnly = true)
    public void validateCountryCurrency(String countryCode, String currencyCode, String transactionId) {
        log.debug("Validating currency-country combination: {} - {} for transaction: {}", 
            currencyCode, countryCode, transactionId);
        
        // Validate currency exists
        Optional<Currency> currency = getCurrencyByCode(currencyCode);
        if (currency.isEmpty()) {
            throw new ValidationException(
                "Invalid currency code: " + currencyCode,
                "INVALID_CURRENCY",
                "currency",
                transactionId
            );
        }
        
        // Validate country exists
        Optional<Country> country = getCountryByCode(countryCode);
        if (country.isEmpty()) {
            throw new ValidationException(
                "Invalid country code: " + countryCode,
                "INVALID_COUNTRY",
                "country",
                transactionId
            );
        }
        
        // Validate currency-country combination
        if (!isValidCurrencyCountry(currencyCode, countryCode)) {
            throw new ValidationException(
                String.format("Currency %s is not valid for country %s", currencyCode, countryCode),
                "INVALID_CURRENCY_COUNTRY_COMBINATION",
                "currency",
                transactionId
            );
        }
        
        log.debug("Currency-country validation successful: {} - {} for transaction: {}", 
            currencyCode, countryCode, transactionId);
    }
    
    /**
     * Get currency by code with caching
     */
    @Cacheable(value = "currencyCache", key = "#currencyCode")
    @Transactional(readOnly = true)
    public Optional<Currency> getCurrencyByCode(String currencyCode) {
        log.debug("Fetching currency by code: {}", currencyCode);
        return currencyRepository.findByCode(currencyCode);
    }
    
    /**
     * Get country by code with caching
     */
    @Cacheable(value = "countryCache", key = "#countryCode")
    @Transactional(readOnly = true)
    public Optional<Country> getCountryByCode(String countryCode) {
        log.debug("Fetching country by code: {}", countryCode);
        return countryRepository.findByCode(countryCode);
    }
    
    /**
     * Check if currency-country combination is valid
     */
    @Transactional(readOnly = true)
    public boolean isValidCurrencyCountry(String currencyCode, String countryCode) {
        log.debug("Checking validity of currency-country combination: {} - {}", currencyCode, countryCode);
        
        Optional<Currency> currency = getCurrencyByCode(currencyCode);
        Optional<Country> country = getCountryByCode(countryCode);
        
        if (currency.isEmpty() || country.isEmpty()) {
            return false;
        }
        
        // Check if the currency is valid for the country
        // This is a simplified validation - in production, you might have a separate mapping table
        return currency.get().getValidCountries().contains(countryCode);
    }
    
    /**
     * Get valid currencies for a country
     */
    @Transactional(readOnly = true)
    public List<String> getValidCurrenciesForCountry(String countryCode) {
        log.debug("Fetching valid currencies for country: {}", countryCode);
        
        Optional<Country> country = getCountryByCode(countryCode);
        if (country.isEmpty()) {
            return List.of();
        }
        
        return country.get().getValidCurrencies();
    }
    
    /**
     * Get valid countries for a currency
     */
    @Transactional(readOnly = true)
    public List<String> getValidCountriesForCurrency(String currencyCode) {
        log.debug("Fetching valid countries for currency: {}", currencyCode);
        
        Optional<Currency> currency = getCurrencyByCode(currencyCode);
        if (currency.isEmpty()) {
            return List.of();
        }
        
        return currency.get().getValidCountries();
    }
    
    /**
     * Refresh currency cache
     */
    @CacheEvict(value = "currencyCache", allEntries = true)
    public void refreshCurrencyCache() {
        log.info("Currency cache refreshed");
    }
    
    /**
     * Refresh country cache
     */
    @CacheEvict(value = "countryCache", allEntries = true)
    public void refreshCountryCache() {
        log.info("Country cache refreshed");
    }
    
    /**
     * Refresh all validation caches
     */
    public void refreshAllCaches() {
        refreshCurrencyCache();
        refreshCountryCache();
        log.info("All validation caches refreshed");
    }
}
