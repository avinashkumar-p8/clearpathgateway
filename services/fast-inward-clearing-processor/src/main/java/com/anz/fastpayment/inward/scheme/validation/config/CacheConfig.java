package com.anz.fastpayment.inward.scheme.validation.config;

import org.springframework.cache.CacheManager;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.cache.concurrent.ConcurrentMapCacheManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Cache Configuration for Banking Operations
 * Uses in-memory cache for better performance and simplicity
 */
@Configuration
@EnableCaching
public class CacheConfig {

    /**
     * In-Memory Cache Manager for Banking Operations
     * Provides fast, thread-safe caching without external dependencies
     */
    @Bean
    public CacheManager cacheManager() {
        ConcurrentMapCacheManager cacheManager = new ConcurrentMapCacheManager();
        
        // Configure cache names for different types of data
        cacheManager.setCacheNames(java.util.Arrays.asList(
            "muidCache",        // Message Unique ID cache
            "currencyCache",    // Currency validation cache
            "countryCache"      // Country validation cache
        ));
        
        // Enable cache statistics and monitoring
        cacheManager.setAllowNullValues(true);
        
        return cacheManager;
    }
}
