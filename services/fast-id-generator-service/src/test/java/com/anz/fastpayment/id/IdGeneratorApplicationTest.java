package com.anz.fastpayment.id;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cache.CacheManager;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
class IdGeneratorApplicationTest {

    @Autowired
    private CacheManager cacheManager;

    @Test
    void contextLoads_andCacheManagerPresent() {
        assertNotNull(cacheManager);
    }
}
