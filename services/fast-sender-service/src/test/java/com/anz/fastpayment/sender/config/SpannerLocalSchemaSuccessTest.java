package com.anz.fastpayment.sender.config;

import com.google.cloud.spring.data.spanner.core.admin.SpannerDatabaseAdminTemplate;
import org.junit.jupiter.api.Test;

import static org.mockito.Mockito.*;

class SpannerLocalSchemaSuccessTest {
    @Test
    void happyPath_executesDdl() {
        SpannerDatabaseAdminTemplate admin = mock(SpannerDatabaseAdminTemplate.class);
        SpannerLocalSchema s = new SpannerLocalSchema(admin);
        s.ensureTables();
        verify(admin, atLeastOnce()).executeDdlStrings(anyList(), eq(true));
    }
}


