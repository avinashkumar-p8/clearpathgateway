package com.anz.fastpayment.router.service;

import com.google.cloud.spring.data.spanner.core.SpannerTemplate;
import org.junit.jupiter.api.Test;
import org.springframework.dao.DuplicateKeyException;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class DuplicateCheckerMoreBranchesTest {

    @Test
    void duplicateKeyExceptionIsDuplicate() {
        SpannerTemplate tpl = mock(SpannerTemplate.class);
        doThrow(new DuplicateKeyException("dup"))
                .when(tpl).insert(any());
        DuplicateChecker dc = new DuplicateChecker(tpl);
        assertTrue(dc.isDuplicateAndRecord("pacs.003.001.11", "U1", "<x/>"));
    }

    @Test
    void alreadyExistsRuntimeIsDuplicate() {
        SpannerTemplate tpl = mock(SpannerTemplate.class);
        doThrow(new RuntimeException("ALREADY_EXISTS: key"))
                .when(tpl).insert(any());
        DuplicateChecker dc = new DuplicateChecker(tpl);
        assertTrue(dc.isDuplicateAndRecord("pacs.003.001.11", "U2", "<x/>") );
    }

    @Test
    void cacheTtlExpiryRemovesAndTreatsAsFresh() throws Exception {
        SpannerTemplate tpl = mock(SpannerTemplate.class);
        // Succeed insert first time
        DuplicateChecker dc = new DuplicateChecker(tpl, 0L); // ttl 0 minutes
        assertFalse(dc.isDuplicateAndRecord("pacs.003.001.11", "U3", "<x/>") );
        // Wait to ensure now - ts > 0 so 0ms TTL treated expired
        Thread.sleep(2);
        // Immediate second call should re-insert, not duplicate
        assertFalse(dc.isDuplicateAndRecord("pacs.003.001.11", "U3", "<x/>") );
    }
}
