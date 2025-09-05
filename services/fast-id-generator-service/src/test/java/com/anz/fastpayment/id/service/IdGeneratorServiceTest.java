package com.anz.fastpayment.id.service;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import com.anz.fastpayment.id.service.SequenceAllocator;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.BeforeEach;

@SpringBootTest
class IdGeneratorServiceTest {

    @Autowired
    private IdGeneratorService svc;

    @MockBean
    private SequenceAllocator allocator;

    @BeforeEach
    void setupAllocator() {
        final long[] next = {1L};
        org.mockito.Mockito.reset(allocator);
        org.mockito.Mockito.when(allocator.allocateBlock(org.mockito.Mockito.anyInt()))
                .thenAnswer(inv -> {
                    long start = next[0];
                    next[0] += 1000L;
                    return start;
                });
    }

    @Test
    void puid_hasLength16_andPrefix_G31() {
        String id = svc.nextPuid("G31");
        assertEquals(16, id.length());
        assertTrue(id.startsWith("G31"));
    }

    @Test
    void block_generation_produces_unique_ids() {
        List<String> ids = svc.nextPuidBlock("G31", 50);
        Set<String> set = new HashSet<>(ids);
        assertEquals(ids.size(), set.size());
    }

    @Test
    void sequential_ids_within_single_block_are_unique() {
        Set<String> set = new HashSet<>();
        for (int i = 0; i < 1000; i++) {
            String id = svc.nextPuid("G31");
            set.add(id);
        }
        assertEquals(1000, set.size());
    }

    @Test
    void muid_has_prefix_MSG_and_length16() {
        String muid = svc.nextMuid();
        assertEquals(16, muid.length());
        assertTrue(muid.startsWith("MSG"));
    }
}


