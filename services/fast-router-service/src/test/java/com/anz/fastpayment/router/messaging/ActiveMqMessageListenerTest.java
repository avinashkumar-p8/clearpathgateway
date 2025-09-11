package com.anz.fastpayment.router.messaging;

import com.anz.fastpayment.router.service.RouterOrchestrator;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class ActiveMqMessageListenerTest {

    @Test
    void ignoresNullAndBlank() {
        RouterOrchestrator orch = mock(RouterOrchestrator.class);
        ActiveMqMessageListener l = new ActiveMqMessageListener(orch);
        l.onMessage(null);
        l.onMessage("   ");
        verify(orch, never()).processInboundXml(any());
    }

    @Test
    void processesValidPayload() throws Exception {
        RouterOrchestrator orch = mock(RouterOrchestrator.class);
        ActiveMqMessageListener l = new ActiveMqMessageListener(orch);
        ReflectionTestUtils.setField(l, "payloadPreviewEnabled", true);
        l.onMessage("<x>123456789</x>");
        verify(orch, atLeastOnce()).processInboundXml(any());
    }

    @Test
    void rethrowsOnException() throws Exception {
        RouterOrchestrator orch = mock(RouterOrchestrator.class);
        doThrow(new RuntimeException("boom")).when(orch).processInboundXml(any());
        ActiveMqMessageListener l = new ActiveMqMessageListener(orch);
        assertThrows(RuntimeException.class, () -> l.onMessage("<x/>"));
    }
}
