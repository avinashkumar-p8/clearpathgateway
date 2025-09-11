package com.anz.fastpayment.router.service;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class PuidGeneratorTest {

    @Test
    void nextPuidThrowsWhenIdServiceUnavailable() {
        PuidGenerator g = new PuidGenerator();
        assertThrows(IllegalStateException.class, g::nextPuid);
    }
}
