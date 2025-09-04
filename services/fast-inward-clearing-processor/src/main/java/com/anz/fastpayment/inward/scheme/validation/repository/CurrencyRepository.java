package com.anz.fastpayment.inward.scheme.validation.repository;

import com.anz.fastpayment.inward.scheme.validation.entity.Currency;
import com.google.cloud.spring.data.spanner.repository.SpannerRepository;
import com.google.cloud.spring.data.spanner.repository.query.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

/**
 * Repository interface for Currency entity operations
 * Provides data access methods for currency validation and lookup
 * Uses Google Cloud Spanner for data persistence
 */
@Repository
public interface CurrencyRepository extends SpannerRepository<Currency, Long> {
    
    /**
     * Find currency by code
     * @param code The currency code (e.g., "SGD", "USD")
     * @return Optional containing the currency if found
     */
    @Query("SELECT * FROM currencies WHERE code = @code")
    Optional<Currency> findByCode(@Param("code") String code);
    
    /**
     * Find all active currencies
     * @return List of active currencies
     */
    @Query("SELECT * FROM currencies WHERE is_active = true")
    List<Currency> findByActiveTrue();
    
    /**
     * Find currencies by region/country
     * @param countryCode The country code to search for
     * @return List of currencies valid for the given country
     */
    @Query("SELECT * FROM currencies WHERE @countryCode IN UNNEST(valid_countries)")
    List<Currency> findByCountryCode(@Param("countryCode") String countryCode);
    
    /**
     * Check if currency exists by code
     * @param code The currency code to check
     * @return true if currency exists, false otherwise
     */
    @Query("SELECT COUNT(*) > 0 FROM currencies WHERE code = @code")
    boolean existsByCode(@Param("code") String code);
    
    /**
     * Find currencies by name (case-insensitive)
     * @param name The currency name to search for
     * @return List of currencies matching the name
     */
    @Query("SELECT * FROM currencies WHERE LOWER(name) LIKE LOWER(@name)")
    List<Currency> findByNameContainingIgnoreCase(@Param("name") String name);
    
    /**
     * Find currencies by symbol
     * @param symbol The currency symbol to search for
     * @return List of currencies with the given symbol
     */
    @Query("SELECT * FROM currencies WHERE symbol = @symbol")
    List<Currency> findBySymbol(@Param("symbol") String symbol);
    
    /**
     * Find active currencies by decimal places
     * @param decimalPlaces The number of decimal places
     * @return List of active currencies with the specified decimal places
     */
    @Query("SELECT * FROM currencies WHERE decimal_places = @decimalPlaces AND is_active = true")
    List<Currency> findByDecimalPlacesAndActiveTrue(@Param("decimalPlaces") Integer decimalPlaces);
    
    /**
     * Count total number of currencies
     * @return Total count of currencies
     */
    @Query("SELECT COUNT(*) FROM currencies")
    long countAll();
    
    /**
     * Count active currencies
     * @return Count of active currencies
     */
    @Query("SELECT COUNT(*) FROM currencies WHERE is_active = true")
    long countActive();
}
