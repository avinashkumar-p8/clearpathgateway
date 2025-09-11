package com.anz.fastpayment.router.config;

import com.google.cloud.spring.data.spanner.core.admin.SpannerDatabaseAdminTemplate;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class SpannerLocalSchemaRetriesTest {

    @Test
    void ensureTables_handlesAlreadyExists_and_singleRetryThenSuccess() {
        SpannerDatabaseAdminTemplate admin = mock(SpannerDatabaseAdminTemplate.class);
        // First DDL call: already exists branch; subsequent succeed
        doThrow(new RuntimeException("ALREADY_EXISTS: table exists"))
                .doNothing()
                .doNothing()
                .doNothing()
                .doNothing()
                .when(admin).executeDdlStrings(anyList(), eq(true));

        SpannerLocalSchema schema = new SpannerLocalSchema(admin);
        org.springframework.test.util.ReflectionTestUtils.setField(schema, "instanceId", "test-instance");
        org.springframework.test.util.ReflectionTestUtils.setField(schema, "databaseId", "test-db");
        org.springframework.test.util.ReflectionTestUtils.setField(schema, "projectId", "local-project");

        // Force one init failure to exercise outer retry
        org.springframework.test.util.ReflectionTestUtils.invokeMethod(schema, "setTestInitFailCount", 1);
        assertDoesNotThrow(schema::ensureTables);
        verify(admin, atLeast(4)).executeDdlStrings(anyList(), eq(true));
    }
}


