package com.anz.fastpayment.router.service;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class UniqueIdExtractorNullTypeTest {
    private final UniqueIdExtractor extractor = new UniqueIdExtractor();

    @Test
    void nullTypeFallsBackToMsgId() {
        String xml = "<Document><GrpHdr><MsgId>MZ</MsgId></GrpHdr></Document>";
        assertEquals("MZ", extractor.extractUniqueId(xml, null));
    }

    @Test
    void nullTypeNoTagsReturnsEmpty() {
        assertEquals("", extractor.extractUniqueId("<x/>", null));
    }
}
