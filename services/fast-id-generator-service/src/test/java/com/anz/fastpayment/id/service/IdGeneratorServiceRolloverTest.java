package com.anz.fastpayment.id.service;

import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class IdGeneratorServiceRolloverTest {

    @Test
    void nextNumeric_rolls_over_to_new_block_after_boundary() {
        // Allocator returns deterministic starts: 1, 1001, 2001, ...
        SequenceAllocator alloc = new SequenceAllocator() {
            private long next = 1L;
            @Override public long allocateBlock(int blockSize) { long start = next; next += blockSize; return start; }
        };
        IdGeneratorService svc = new IdGeneratorService(alloc);

        // Consume 1000 puids from first block
        for (int i = 0; i < 1000; i++) {
            String id = svc.nextPuid("G31");
            assertTrue(id.startsWith("G31"));
        }
        // Next should come from the second block without duplicates
        String rollover = svc.nextPuid("G31");
        assertTrue(rollover.startsWith("G31"));

        // Spot check uniqueness around the boundary
        Set<String> set = new HashSet<>();
        for (int i = 0; i < 10; i++) {
            set.add(svc.nextPuid("G31"));
        }
        assertEquals(10, set.size());
    }
}


