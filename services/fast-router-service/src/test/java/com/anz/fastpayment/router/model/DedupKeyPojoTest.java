package com.anz.fastpayment.router.model;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;

class DedupKeyPojoTest {
    @Test
    void gettersAndSettersWork() {
        DedupKey k = new DedupKey();
        k.setMessageType("pacs.003.001.11");
        k.setUniqueId("U1");
        assertEquals("pacs.003.001.11", k.getMessageType());
        assertEquals("U1", k.getUniqueId());
    }
}
