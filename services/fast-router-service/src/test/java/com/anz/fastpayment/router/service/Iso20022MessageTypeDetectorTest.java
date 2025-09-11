package com.anz.fastpayment.router.service;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;

class Iso20022MessageTypeDetectorTest {

    private final Iso20022MessageTypeDetector detector = new Iso20022MessageTypeDetector();

    @Test
    void detectsKnownTypesAndUnknown() {
        String x = "<x>pacs.008.001.13</x>";
        assertEquals("pacs.008.001.13", detector.detectType(x));
        assertEquals("pacs.003.001.11", detector.detectType("pacs.003.001.11"));
        assertEquals("pacs.007.001.13", detector.detectType("pacs.007.001.13"));
        assertEquals("camt.056.001.11", detector.detectType("camt.056.001.11"));
        // camt.029 is not detected by router; it is generated in sender
        assertEquals("head.001.001.01", detector.detectType("head.001.001.01"));
        assertEquals("unknown", detector.detectType("<x/>"));
        assertEquals("unknown", detector.detectType(null));
    }
}
