package com.anz.fastpayment.id.service;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class IdGeneratorServiceNegativeTest {

    @Test
    void negative_start_numbers_are_made_positive_in_output() {
        SequenceAllocator alloc = new SequenceAllocator() {
            private long v = -100L;
            @Override public long allocateBlock(int blockSize) { long s = v; v += blockSize; return s; }
        };
        IdGeneratorService svc = new IdGeneratorService(alloc);
        String puid = svc.nextPuid("G31");
        assertEquals(16, puid.length());
        assertTrue(puid.startsWith("G31"));
        // ensure only digits after prefix
        assertTrue(puid.substring(3).matches("\\d{13}"));
    }
}


