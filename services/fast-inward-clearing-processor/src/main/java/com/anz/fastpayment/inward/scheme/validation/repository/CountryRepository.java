package com.anz.fastpayment.inward.scheme.validation.repository;

import com.anz.fastpayment.inward.scheme.validation.entity.Country;
import com.google.cloud.spring.data.spanner.repository.SpannerRepository;
import com.google.cloud.spring.data.spanner.repository.query.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

/**
 * Repository interface for Country entity operations
 * Provides data access methods for country validation and lookup
 * Uses Google Cloud Spanner for data persistence
 */
@Repository
public interface CountryRepository extends SpannerRepository<Country, Long> {
    
    /**
     * Find country by code
     * @param code The country code (e.g., "SG", "US")
     * @return Optional containing the country if found
     */
    @Query("SELECT * FROM countries WHERE code = @code")
    Optional<Country> findByCode(@Param("code") String code);
    
    /**
     * Find country by ISO code
     * @param isoCode The ISO country code (e.g., "SGP", "USA")
     * @return Optional containing the country if found
     */
    @Query("SELECT * FROM countries WHERE iso_code = @isoCode")
    Optional<Country> findByIsoCode(@Param("isoCode") String isoCode);
    
    /**
     * Find all active countries
     * @return List of active countries
     */
    @Query("SELECT * FROM countries WHERE is_active = true")
    List<Country> findByActiveTrue();
    
    /**
     * Find countries by currency code
     * @param currencyCode The currency code to search for
     * @return List of countries using the given currency
     */
    @Query("SELECT * FROM countries WHERE currency_code = @currencyCode")
    List<Country> findByCurrencyCode(@Param("currencyCode") String currencyCode);
    
    /**
     * Check if country exists by code
     * @param code The country code to check
     * @return true if country exists, false otherwise
     */
    @Query("SELECT COUNT(*) > 0 FROM countries WHERE code = @code")
    boolean existsByCode(@Param("code") String code);
    
    /**
     * Check if country exists by ISO code
     * @param isoCode The ISO country code to check
     * @return true if country exists, false otherwise
     */
    @Query("SELECT COUNT(*) > 0 FROM countries WHERE iso_code = @isoCode")
    boolean existsByIsoCode(@Param("isoCode") String isoCode);
    
    /**
     * Find countries by name (case-insensitive)
     * @param name The country name to search for
     * @return List of countries matching the name
     */
    @Query("SELECT * FROM countries WHERE LOWER(name) LIKE LOWER(@name)")
    List<Country> findByNameContainingIgnoreCase(@Param("name") String name);
    
    /**
     * Find active countries by currency code
     * @param currencyCode The currency code to search for
     * @return List of active countries using the given currency
     */
    @Query("SELECT * FROM countries WHERE currency_code = @currencyCode AND is_active = true")
    List<Country> findByCurrencyCodeAndActiveTrue(@Param("currencyCode") String currencyCode);
    
    /**
     * Count total number of countries
     * @return Total count of countries
     */
    @Query("SELECT COUNT(*) FROM countries")
    long countAll();
    
    /**
     * Count active countries
     * @return Count of active countries
     */
    @Query("SELECT COUNT(*) FROM countries WHERE is_active = true")
    long countActive();
    
    /**
     * Find countries with specific validation rules
     * @param ruleName The validation rule name to search for
     * @return List of countries that have the specified validation rule
     */
    @Query("SELECT * FROM countries WHERE @ruleName IN UNNEST(validation_rules)")
    List<Country> findByValidationRule(@Param("ruleName") String ruleName);
}
