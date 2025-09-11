package com.anz.fastpayment.router.messaging;

import com.anz.fastpayment.router.service.RouterOrchestrator;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class AbstractJmsMessageListenerUtilTest {

    static class Stub extends AbstractJmsMessageListener {
        Stub(RouterOrchestrator o) { super(o); }
        String call(String s, int n) { return safePreview(s, n); }
    }

    @Test
    void safePreviewHandlesNullBlankMaskingAndLength() {
        Stub s = new Stub(null);
        assertEquals("<empty>", s.call(null, 10));
        assertEquals("<blank>", s.call("   ", 10));
        String masked = s.call("<IBAN>DE12345678901234567890</IBAN> acct <AcctNbr>12345678</AcctNbr> 999999999 Name", 200);
        assertTrue(masked.contains("***masked***"));
        String cut = s.call("abcdefghij", 5);
        assertEquals("abcde", cut);
    }
}
