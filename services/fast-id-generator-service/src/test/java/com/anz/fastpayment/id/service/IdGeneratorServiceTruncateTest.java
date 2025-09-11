package com.anz.fastpayment.id.service;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class IdGeneratorServiceTruncateTest {

    @Test
    void to13Digits_truncates_when_number_exceeds_13_digits() {
        // Start with a number > 13 digits to force truncation path
        final long start = 10_000_000_000_000L + 12345L; // 10^13 +
        SequenceAllocator alloc = new SequenceAllocator() {
            private long v = start;
            @Override public long allocateBlock(int blockSize) { long s = v; v += blockSize; return s; }
        };
        IdGeneratorService svc = new IdGeneratorService(alloc);
        String puid = svc.nextPuid("G31");
        assertEquals(16, puid.length());
        assertTrue(puid.startsWith("G31"));
        // Ensure only last 13 digits are retained after prefix
        String digits = puid.substring(3);
        assertEquals(13, digits.length());
    }
}


