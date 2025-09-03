package com.anz.fastpayment.id;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.cache.caffeine.CaffeineCacheManager;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
@EnableCaching
public class IdGeneratorApplication {
    public static void main(String[] args) {
        SpringApplication.run(IdGeneratorApplication.class, args);
    }

    @Bean
    public CaffeineCacheManager cacheManager() {
        CaffeineCacheManager mgr = new CaffeineCacheManager("muidByPuid");
        // further tuned via properties if needed
        return mgr;
    }
}




