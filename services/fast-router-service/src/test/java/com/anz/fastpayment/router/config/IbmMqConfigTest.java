package com.anz.fastpayment.router.config;

import jakarta.jms.ConnectionFactory;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class IbmMqConfigTest {

    @Test
    void jmsListenerFactoryBuilds() {
        IbmMqConfig cfg = new IbmMqConfig();
        ConnectionFactory cf = mock(ConnectionFactory.class);
        assertNotNull(cfg.jmsListenerContainerFactory(cf));
    }

    @Test
    void connectionFactory_setsOptionalUserPassword() throws Exception {
        IbmMqConfig cfg = new IbmMqConfig();
        org.springframework.test.util.ReflectionTestUtils.setField(cfg, "host", "h");
        org.springframework.test.util.ReflectionTestUtils.setField(cfg, "port", 1);
        org.springframework.test.util.ReflectionTestUtils.setField(cfg, "channel", "c");
        org.springframework.test.util.ReflectionTestUtils.setField(cfg, "queueManager", "q");
        org.springframework.test.util.ReflectionTestUtils.setField(cfg, "user", "u");
        org.springframework.test.util.ReflectionTestUtils.setField(cfg, "password", "p");
        assertNotNull(cfg.ibmMqConnectionFactory());
    }

    @Test
    void connectionFactory_withoutCredentials() throws Exception {
        IbmMqConfig cfg = new IbmMqConfig();
        org.springframework.test.util.ReflectionTestUtils.setField(cfg, "host", "h");
        org.springframework.test.util.ReflectionTestUtils.setField(cfg, "port", 1);
        org.springframework.test.util.ReflectionTestUtils.setField(cfg, "channel", "c");
        org.springframework.test.util.ReflectionTestUtils.setField(cfg, "queueManager", "q");
        // user and password left null/blank to exercise false branches
        assertNotNull(cfg.ibmMqConnectionFactory());
    }
}
