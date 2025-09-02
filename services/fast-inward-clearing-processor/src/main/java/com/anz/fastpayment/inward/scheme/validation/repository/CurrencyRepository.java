package com.anz.fastpayment.inward.scheme.validation.repository;

import com.anz.fastpayment.inward.scheme.validation.entity.Currency;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

/**
 * Repository for Currency entity operations
 * Provides data access methods for currency validation
 */
@Repository
public interface CurrencyRepository extends JpaRepository<Currency, Long> {
    
    /**
     * Find currency by code
     */
    Optional<Currency> findByCode(String code);
    
    /**
     * Find active currencies
     */
    List<Currency> findByActiveTrue();
    
    /**
     * Find currencies by region
     */
    @Query("SELECT c FROM Currency c WHERE c.validCountries LIKE %:countryCode%")
    List<Currency> findByCountryCode(@Param("countryCode") String countryCode);
    
    /**
     * Check if currency exists by code
     */
    boolean existsByCode(String code);
}
