package com.anz.fastpayment.sender.config;

import com.google.cloud.spring.data.spanner.core.admin.SpannerDatabaseAdminTemplate;
import com.google.cloud.spanner.ErrorCode;
import com.google.cloud.spanner.SpannerException;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class SpannerLocalSchemaBranchesTest {
    @Test
    void alreadyExists_isCaught() {
        SpannerDatabaseAdminTemplate admin = mock(SpannerDatabaseAdminTemplate.class);
        SpannerException se = mock(SpannerException.class);
        when(se.getErrorCode()).thenReturn(ErrorCode.ALREADY_EXISTS);
        doThrow(se).when(admin).executeDdlStrings(anyList(), eq(true));
        SpannerLocalSchema s = new SpannerLocalSchema(admin);
        s.ensureTables();
        verify(admin, atLeastOnce()).executeDdlStrings(anyList(), eq(true));
    }

    @Test
    void otherException_isWarned() {
        SpannerDatabaseAdminTemplate admin = mock(SpannerDatabaseAdminTemplate.class);
        doThrow(new RuntimeException("boom")).when(admin).executeDdlStrings(anyList(), eq(true));
        SpannerLocalSchema s = new SpannerLocalSchema(admin);
        s.ensureTables();
        verify(admin, atLeastOnce()).executeDdlStrings(anyList(), eq(true));
    }
}


