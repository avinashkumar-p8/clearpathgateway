package com.anz.fastpayment.router.config;

import com.google.cloud.spring.data.spanner.core.admin.SpannerDatabaseAdminTemplate;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class SpannerLocalSchemaDdlRetryOnceTest {

    @Test
    void retriesOncePerTable_thenSucceeds() {
        SpannerDatabaseAdminTemplate admin = mock(SpannerDatabaseAdminTemplate.class);
        // There are 4 DDLs; for each first call throw generic error (not already exists), then succeed
        // Total executeDdlStrings invocations = 8
        doThrow(new RuntimeException("temporary"))
                .doNothing()
                .doThrow(new RuntimeException("temporary"))
                .doNothing()
                .doThrow(new RuntimeException("temporary"))
                .doNothing()
                .doThrow(new RuntimeException("temporary"))
                .doNothing()
                .when(admin).executeDdlStrings(anyList(), eq(true));

        SpannerLocalSchema schema = new SpannerLocalSchema(admin);
        org.springframework.test.util.ReflectionTestUtils.setField(schema, "instanceId", "test-instance");
        org.springframework.test.util.ReflectionTestUtils.setField(schema, "databaseId", "test-db");
        org.springframework.test.util.ReflectionTestUtils.setField(schema, "projectId", "local-project");

        assertDoesNotThrow(schema::ensureTables);
        // Verify at least one retry happened
        verify(admin, atLeast(8)).executeDdlStrings(anyList(), eq(true));
    }
}


