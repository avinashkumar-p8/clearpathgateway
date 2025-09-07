package com.anz.fastpayment.id;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.cache.CacheManager;
import org.springframework.cache.jcache.JCacheCacheManager;
import org.springframework.context.annotation.Primary;
import javax.cache.Caching;
import javax.cache.spi.CachingProvider;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
@EnableCaching
public class IdGeneratorApplication {
    public static void main(String[] args) {
        SpringApplication.run(IdGeneratorApplication.class, args);
    }

    @Bean
    @Primary
    public CacheManager cacheManager() {
        CachingProvider provider = Caching.getCachingProvider();
        javax.cache.CacheManager jCacheManager = provider.getCacheManager();
        return new JCacheCacheManager(jCacheManager);
    }
}




