package com.anz.fastpayment.inward.scheme.validation.repository;

import com.anz.fastpayment.inward.scheme.validation.entity.Country;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

/**
 * Repository for Country entity operations
 * Provides data access methods for country validation
 */
@Repository
public interface CountryRepository extends JpaRepository<Country, Long> {
    
    /**
     * Find country by code
     */
    Optional<Country> findByCode(String code);
    
    /**
     * Find country by ISO code
     */
    Optional<Country> findByIsoCode(String isoCode);
    
    /**
     * Find active countries
     */
    List<Country> findByActiveTrue();
    
    /**
     * Find countries by region
     */
    List<Country> findByRegion(String region);
    
    /**
     * Find countries by currency code
     */
    @Query("SELECT c FROM Country c WHERE c.validCurrencies LIKE %:currencyCode%")
    List<Country> findByCurrencyCode(@Param("currencyCode") String currencyCode);
    
    /**
     * Check if country exists by code
     */
    boolean existsByCode(String code);
}
