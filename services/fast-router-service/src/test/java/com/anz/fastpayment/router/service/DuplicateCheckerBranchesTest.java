package com.anz.fastpayment.router.service;

import com.anz.fastpayment.router.model.DedupKey;
import com.google.cloud.spring.data.spanner.core.SpannerTemplate;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class DuplicateCheckerBranchesTest {

    @Test
    void returnsFalseWhenUniqueIdBlank() {
        SpannerTemplate tpl = mock(SpannerTemplate.class);
        DuplicateChecker dc = new DuplicateChecker(tpl);
        assertFalse(dc.isDuplicateAndRecord("pacs.008.001.13", " ", "<xml/>"));
    }

    @Test
    void cacheHitReturnsTrue() {
        SpannerTemplate tpl = mock(SpannerTemplate.class);
        DuplicateChecker dc = new DuplicateChecker(tpl, 60);
        assertFalse(dc.isDuplicateAndRecord("pacs.008.001.13", "E2E-1", "<xml/>") );
        assertTrue(dc.isDuplicateAndRecord("pacs.008.001.13", "E2E-1", "<xml/>") );
        verify(tpl, times(1)).insert(any(DedupKey.class));
    }

    @Test
    void nonFatalInsertFailureSeedsCacheAndReturnsFalse() {
        SpannerTemplate tpl = mock(SpannerTemplate.class);
        doThrow(new RuntimeException("network glitch"))
                .when(tpl).insert(any(DedupKey.class));
        DuplicateChecker dc = new DuplicateChecker(tpl, 60);
        assertFalse(dc.isDuplicateAndRecord("pacs.008.001.13", "E2E-2", "<xml/>") );
        // Second call should be true due to cache seeding even on failure
        assertTrue(dc.isDuplicateAndRecord("pacs.008.001.13", "E2E-2", "<xml/>") );
    }
}
