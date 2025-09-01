package com.anz.fastpayment.inward.service;

import com.anz.fastpayment.inward.scheme.validation.entity.Country;
import com.anz.fastpayment.inward.scheme.validation.entity.Currency;
import com.anz.fastpayment.inward.scheme.validation.exception.ValidationException;
import com.anz.fastpayment.inward.scheme.validation.repository.CountryRepository;
import com.anz.fastpayment.inward.scheme.validation.repository.CurrencyRepository;
import com.anz.fastpayment.inward.scheme.validation.service.CurrencyCountryValidationService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

/**
 * Test class for CurrencyCountryValidationService
 * Tests currency-country validation logic with mocked repositories
 */
@ExtendWith(MockitoExtension.class)
class CurrencyCountryValidationServiceTest {

    @Mock
    private CurrencyRepository currencyRepository;

    @Mock
    private CountryRepository countryRepository;

    @InjectMocks
    private CurrencyCountryValidationService validationService;

    private Currency usdCurrency;
    private Currency sgdCurrency;
    private Country usCountry;
    private Country sgCountry;

    @BeforeEach
    void setUp() {
        // Setup test data
        usdCurrency = new Currency("USD", "US Dollar");
        usdCurrency.setId(1L);
        usdCurrency.setSymbol("$");
        usdCurrency.setValidCountries(Arrays.asList("US", "CA", "AU"));

        sgdCurrency = new Currency("SGD", "Singapore Dollar");
        sgdCurrency.setId(2L);
        sgdCurrency.setSymbol("S$");
        sgdCurrency.setValidCountries(Arrays.asList("SG", "MY"));

        usCountry = new Country("US", "United States");
        usCountry.setId(1L);
        usCountry.setIsoCode("USA");
        usCountry.setValidCurrencies(Arrays.asList("USD"));

        sgCountry = new Country("SG", "Singapore");
        sgCountry.setId(2L);
        sgCountry.setIsoCode("SGP");
        sgCountry.setValidCurrencies(Arrays.asList("SGD"));
    }

    @Test
    void testValidateCountryCurrency_ValidCombination() {
        // Arrange
        when(currencyRepository.findByCode("USD")).thenReturn(Optional.of(usdCurrency));
        when(countryRepository.findByCode("US")).thenReturn(Optional.of(usCountry));

        // Act & Assert
        assertDoesNotThrow(() -> 
            validationService.validateCountryCurrency("US", "USD", "TXN-001"));
    }

    @Test
    void testValidateCountryCurrency_InvalidCurrency() {
        // Arrange
        when(currencyRepository.findByCode("INVALID")).thenReturn(Optional.empty());

        // Act & Assert
        ValidationException exception = assertThrows(ValidationException.class, () ->
            validationService.validateCountryCurrency("US", "INVALID", "TXN-001"));

        assertEquals("INVALID_CURRENCY", exception.getErrorCode());
        assertEquals("currency", exception.getField());
        assertEquals("TXN-001", exception.getTransactionId());
    }

    @Test
    void testValidateCountryCurrency_InvalidCountry() {
        // Arrange
        when(currencyRepository.findByCode("USD")).thenReturn(Optional.of(usdCurrency));
        when(countryRepository.findByCode("INVALID")).thenReturn(Optional.empty());

        // Act & Assert
        ValidationException exception = assertThrows(ValidationException.class, () ->
            validationService.validateCountryCurrency("INVALID", "USD", "TXN-001"));

        assertEquals("INVALID_COUNTRY", exception.getErrorCode());
        assertEquals("country", exception.getField());
        assertEquals("TXN-001", exception.getTransactionId());
    }

    @Test
    void testValidateCountryCurrency_InvalidCombination() {
        // Arrange
        when(currencyRepository.findByCode("USD")).thenReturn(Optional.of(usdCurrency));
        when(countryRepository.findByCode("SG")).thenReturn(Optional.of(sgCountry));

        // Act & Assert
        ValidationException exception = assertThrows(ValidationException.class, () ->
            validationService.validateCountryCurrency("SG", "USD", "TXN-001"));

        assertEquals("INVALID_CURRENCY_COUNTRY_COMBINATION", exception.getErrorCode());
        assertEquals("currency", exception.getField());
        assertEquals("TXN-001", exception.getTransactionId());
    }

    @Test
    void testGetCurrencyByCode_ValidCurrency() {
        // Arrange
        when(currencyRepository.findByCode("USD")).thenReturn(Optional.of(usdCurrency));

        // Act
        Optional<Currency> result = validationService.getCurrencyByCode("USD");

        // Assert
        assertTrue(result.isPresent());
        assertEquals("USD", result.get().getCode());
        assertEquals("US Dollar", result.get().getName());
    }

    @Test
    void testGetCurrencyByCode_InvalidCurrency() {
        // Arrange
        when(currencyRepository.findByCode("INVALID")).thenReturn(Optional.empty());

        // Act
        Optional<Currency> result = validationService.getCurrencyByCode("INVALID");

        // Assert
        assertFalse(result.isPresent());
    }

    @Test
    void testGetCountryByCode_ValidCountry() {
        // Arrange
        when(countryRepository.findByCode("US")).thenReturn(Optional.of(usCountry));

        // Act
        Optional<Country> result = validationService.getCountryByCode("US");

        // Assert
        assertTrue(result.isPresent());
        assertEquals("US", result.get().getCode());
        assertEquals("United States", result.get().getName());
    }

    @Test
    void testGetCountryByCode_InvalidCountry() {
        // Arrange
        when(countryRepository.findByCode("INVALID")).thenReturn(Optional.empty());

        // Act
        Optional<Country> result = validationService.getCountryByCode("INVALID");

        // Assert
        assertFalse(result.isPresent());
    }

    @Test
    void testIsValidCurrencyCountry_ValidCombination() {
        // Arrange
        when(currencyRepository.findByCode("USD")).thenReturn(Optional.of(usdCurrency));
        when(countryRepository.findByCode("US")).thenReturn(Optional.of(usCountry));

        // Act
        boolean result = validationService.isValidCurrencyCountry("USD", "US");

        // Assert
        assertTrue(result);
    }

    @Test
    void testIsValidCurrencyCountry_InvalidCombination() {
        // Arrange
        when(currencyRepository.findByCode("USD")).thenReturn(Optional.of(usdCurrency));
        when(countryRepository.findByCode("SG")).thenReturn(Optional.of(sgCountry));

        // Act
        boolean result = validationService.isValidCurrencyCountry("USD", "SG");

        // Assert
        assertFalse(result);
    }

    @Test
    void testGetValidCurrenciesForCountry_ValidCountry() {
        // Arrange
        when(countryRepository.findByCode("US")).thenReturn(Optional.of(usCountry));

        // Act
        List<String> result = validationService.getValidCurrenciesForCountry("US");

        // Assert
        assertEquals(1, result.size());
        assertTrue(result.contains("USD"));
    }

    @Test
    void testGetValidCurrenciesForCountry_InvalidCountry() {
        // Arrange
        when(countryRepository.findByCode("INVALID")).thenReturn(Optional.empty());

        // Act
        List<String> result = validationService.getValidCurrenciesForCountry("INVALID");

        // Assert
        assertTrue(result.isEmpty());
    }

    @Test
    void testGetValidCountriesForCurrency_ValidCurrency() {
        // Arrange
        when(currencyRepository.findByCode("USD")).thenReturn(Optional.of(usdCurrency));

        // Act
        List<String> result = validationService.getValidCountriesForCurrency("USD");

        // Assert
        assertEquals(3, result.size());
        assertTrue(result.contains("US"));
        assertTrue(result.contains("CA"));
        assertTrue(result.contains("AU"));
    }

    @Test
    void testGetValidCountriesForCurrency_InvalidCurrency() {
        // Arrange
        when(currencyRepository.findByCode("INVALID")).thenReturn(Optional.empty());

        // Act
        List<String> result = validationService.getValidCountriesForCurrency("INVALID");

        // Assert
        assertTrue(result.isEmpty());
    }

    @Test
    void testCacheEvictionMethods() {
        // Act & Assert - these methods should not throw exceptions
        assertDoesNotThrow(() -> validationService.refreshCurrencyCache());
        assertDoesNotThrow(() -> validationService.refreshCountryCache());
        assertDoesNotThrow(() -> validationService.refreshAllCaches());
    }
}
