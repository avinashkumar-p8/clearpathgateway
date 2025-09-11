package com.anz.fastpayment.id.service;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class IdGeneratorServiceEdgeTest {

    private IdGeneratorService svc;

    @BeforeEach
    void setup() {
        // Deterministic allocator returning fixed starts
        SequenceAllocator alloc = new SequenceAllocator() {
            private long v = 0;
            @Override public long allocateBlock(int blockSize) { long r = v; v += blockSize; return r; }
        };
        svc = new IdGeneratorService(alloc);
    }

    @Test
    void nextPuidBlock_size_clamped_to_minimum_one() {
        assertEquals(1, svc.nextPuidBlock("G31", 0).size());
    }

    @Test
    void to13Digits_padding_and_truncation_covered_via_public_generators() {
        String p1 = svc.nextPuid("G31");
        String m1 = svc.nextMuid();
        assertEquals(16, p1.length());
        assertEquals(16, m1.length());
    }
}
