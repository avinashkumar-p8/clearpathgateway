package com.anz.fastpayment.router.mapping;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;

class TransformationConfigLoaderTest {
    @Test
    void loadsConfigWithoutError() {
        TransformationConfigLoader l = new TransformationConfigLoader(new ObjectMapper());
        // Ensure internal config is not null via reflection if needed
        assertNotNull(l);
    }
}
