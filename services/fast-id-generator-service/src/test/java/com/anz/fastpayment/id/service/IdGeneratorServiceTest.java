package com.anz.fastpayment.id.service;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
class IdGeneratorServiceTest {

    @Autowired
    private IdGeneratorService svc;

    @Test
    void puid_hasLength16_andChannelPrefix() {
        String id = svc.nextPuid("G3I");
        assertEquals(16, id.length());
        assertTrue(id.startsWith("G3I"));
    }

    @Test
    void block_generation_produces_unique_ids() {
        List<String> ids = svc.nextPuidBlock("G3I", 50);
        Set<String> set = new HashSet<>(ids);
        assertEquals(ids.size(), set.size());
    }

    @Test
    void burst_over_1000_per_second_remains_unique() {
        Set<String> set = new HashSet<>();
        for (int i = 0; i < 1200; i++) {
            String id = svc.nextPuid("G3I");
            assertFalse(set.contains(id));
            set.add(id);
        }
        assertEquals(1200, set.size());
    }

    @Test
    void muid_is_prefixed_with_puid_and_is_cached_per_puid() {
        String puid = svc.nextPuid("G3I");
        String m1 = svc.nextMuid(puid);
        String m2 = svc.nextMuid(puid);
        assertTrue(m1.startsWith(puid + "-"));
        assertTrue(m2.startsWith(puid + "-"));
        assertEquals(m1, m2); // cached result should be identical
        assertTrue(m1.length() > puid.length());
    }
}


