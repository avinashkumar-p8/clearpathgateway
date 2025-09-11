package com.anz.fastpayment.router.config;

import com.google.cloud.spring.data.spanner.core.admin.SpannerDatabaseAdminTemplate;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class SpannerLocalSchemaTest {

    @Test
    void ensureTables_invokesDDLWithoutThrow() {
        SpannerDatabaseAdminTemplate admin = mock(SpannerDatabaseAdminTemplate.class);
        SpannerLocalSchema schema = new SpannerLocalSchema(admin);
        // Set required fields via reflection
        org.springframework.test.util.ReflectionTestUtils.setField(schema, "instanceId", "test-instance");
        org.springframework.test.util.ReflectionTestUtils.setField(schema, "databaseId", "test-db");
        org.springframework.test.util.ReflectionTestUtils.setField(schema, "projectId", "local-project");
        assertDoesNotThrow(schema::ensureTables);
        verify(admin, atLeastOnce()).executeDdlStrings(anyList(), eq(true));
    }
}
